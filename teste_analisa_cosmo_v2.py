import pandas as pd
from datetime import datetime, timedelta
from azure.identity import InteractiveBrowserCredential
from azure.mgmt.resource import SubscriptionClient
from azure.mgmt.cosmosdb import CosmosDBManagementClient
from azure.mgmt.monitor import MonitorManagementClient

def get_cosmos_optimized_metrics():
    print("🔔 Autenticando...")
    credential = InteractiveBrowserCredential()
    sub_client = SubscriptionClient(credential)
    results = []

    for sub in sub_client.subscriptions.list():
        sub_id = sub.subscription_id
        print(f"🔍 Analisando Assinatura: {sub.display_name}")

        try:
            cosmos_client = CosmosDBManagementClient(credential, sub_id)
            monitor_client = MonitorManagementClient(credential, sub_id)
            
            for account in cosmos_client.database_accounts.list():
                rg = account.id.split('/')[4]
                print(f"  📂 Conta: {account.name}")

                dbs = list(cosmos_client.sql_resources.list_sql_databases(rg, account.name))
                for db in dbs:
                    # 1. Tentar pegar a métrica no nível do DATABASE (para casos de Shared Throughput)
                    db_resource_id = f"{account.id}/databases/{db.name}"
                    
                    end_time = datetime.utcnow()
                    start_time = end_time - timedelta(days=30)
                    timespan = f"{start_time.isoformat()}Z/{end_time.isoformat()}Z"

                    db_usage = None
                    try:
                        metrics = monitor_client.metrics.list(
                            db_resource_id,
                            timespan=timespan,
                            interval='P1D',
                            metricnames='NormalizedRUConsumption',
                            aggregation='Average'
                        )
                        values = [d.average for item in metrics.value for ts in item.timeseries for d in ts.data if d.average is not None]
                        if values:
                            db_usage = sum(values) / len(values)
                    except:
                        db_usage = None

                    # 2. Listar Containers para detalhar no relatório
                    containers = list(cosmos_client.sql_resources.list_sql_containers(rg, account.name, db.name))
                    for container in containers:
                        coll_usage = db_usage # Assume o uso do banco por padrão
                        tipo_throughput = "Compartilhado (Database)"

                        # 3. Tentar ver se o container tem throughput dedicado (sobrescreve o do banco)
                        try:
                            coll_resource_id = f"{db_resource_id}/containers/{container.name}"
                            c_metrics = monitor_client.metrics.list(
                                coll_resource_id,
                                timespan=timespan,
                                interval='P1D',
                                metricnames='NormalizedRUConsumption',
                                aggregation='Average'
                            )
                            c_values = [d.average for item in c_metrics.value for ts in item.timeseries for d in ts.data if d.average is not None]
                            if c_values:
                                coll_usage = sum(c_values) / len(c_values)
                                tipo_throughput = "Dedicado (Container)"
                        except:
                            pass # Se der erro, mantém o valor do Database

                        if coll_usage is not None and coll_usage < 40:
                            results.append({
                                "Subscription": sub.display_name,
                                "Account": account.name,
                                "Database": db.name,
                                "Collection": container.name,
                                "Tipo_Throughput": tipo_throughput,
                                "Usage_30d_Percent": round(coll_usage, 2)
                            })
                
        except Exception as e:
            print(f"  ❌ Erro na assinatura {sub.display_name}: {str(e)[:100]}")

    if results:
        df = pd.DataFrame(results)
        df.to_csv("auditoria_final_detalhada.csv", index=False, encoding='utf-8-sig')
        print(f"\n✅ Relatório gerado: auditoria_final_detalhada.csv")
        print(df.to_string())
    else:
        print("\n✅ Nenhuma collection ou database subutilizado encontrado.")

if __name__ == "__main__":
    get_cosmos_optimized_metrics()