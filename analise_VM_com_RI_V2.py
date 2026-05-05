import pandas as pd
import numpy as np
from azure.identity import InteractiveBrowserCredential
from azure.mgmt.resource import SubscriptionClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.monitor import MonitorManagementClient
from azure.mgmt.reservations import AzureReservationAPI
from datetime import datetime, timedelta

# --- CONFIGURAÇÕES ---
LOOKBACK_DAYS = 30
CPU_UPGRADE_P95 = 80.0
CPU_DOWNGRADE_P95 = 40.0
MEM_UPGRADE_P95 = 80.0
MEM_DOWNGRADE_P95 = 40.0

# --- CATÁLOGO DE SKUs ---
SKU_CATALOG = {
    # --- Burstable (Série B - Intel) ---
   "Standard_B1s": {"Family": "B", "vCPU": 1, "RAM": 1},
    "Standard_B1ms": {"Family": "B", "vCPU": 1, "RAM": 2},
    "Standard_B2s": {"Family": "B", "vCPU": 2, "RAM": 4},
    "Standard_B2ms": {"Family": "B", "vCPU": 2, "RAM": 8},
    "Standard_B4ms": {"Family": "B", "vCPU": 4, "RAM": 16},
    "Standard_B8ms": {"Family": "B", "vCPU": 8, "RAM": 32},
    "Standard_B12ms": {"Family": "B", "vCPU": 12, "RAM": 48},
    "Standard_B16ms": {"Family": "B", "vCPU": 16, "RAM": 64},
    "Standard_B20ms": {"Family": "B", "vCPU": 20, "RAM": 80},

   # --- NOVO: General Purpose (Série D v3) ---
    "Standard_D2s_v3": {"Family": "Dv3", "vCPU": 2, "RAM": 8},
    "Standard_D4s_v3": {"Family": "Dv3", "vCPU": 4, "RAM": 16},
    "Standard_D8s_v3": {"Family": "Dv3", "vCPU": 8, "RAM": 32},
    "Standard_D16s_v3": {"Family": "Dv3", "vCPU": 16, "RAM": 64},
    "Standard_D32s_v3": {"Family": "Dv3", "vCPU": 32, "RAM": 128},
    "Standard_D48s_v3": {"Family": "Dv3", "vCPU": 48, "RAM": 192},
    "Standard_D64s_v3": {"Family": "Dv3", "vCPU": 64, "RAM": 256},

    # General Purpose (Série D v4)
    "Standard_D2s_v4": {"Family": "Dv4", "vCPU": 2, "RAM": 8},
    "Standard_D4s_v4": {"Family": "Dv4", "vCPU": 4, "RAM": 16},
    "Standard_D8s_v4": {"Family": "Dv4", "vCPU": 8, "RAM": 32},
    "Standard_D16s_v4": {"Family": "Dv4", "vCPU": 16, "RAM": 64},
    "Standard_D32s_v4": {"Family": "Dv4", "vCPU": 32, "RAM": 128},
    "Standard_D48s_v4": {"Family": "Dv4", "vCPU": 48, "RAM": 192},
    "Standard_D64s_v4": {"Family": "Dv4", "vCPU": 64, "RAM": 256},

    # General Purpose (Série D v5)
    "Standard_D2s_v5": {"Family": "Dv5", "vCPU": 2, "RAM": 8},
    "Standard_D4s_v5": {"Family": "Dv5", "vCPU": 4, "RAM": 16},
    "Standard_D8s_v5": {"Family": "Dv5", "vCPU": 8, "RAM": 32},
    "Standard_D16s_v5": {"Family": "Dv5", "vCPU": 16, "RAM": 64},
    "Standard_D32s_v5": {"Family": "Dv5", "vCPU": 32, "RAM": 128},
    "Standard_D48s_v5": {"Family": "Dv5", "vCPU": 48, "RAM": 192},
    "Standard_D64s_v5": {"Family": "Dv5", "vCPU": 64, "RAM": 256},

    # --- NOVO: General Purpose AMD (Série Da v4 / Da v5) ---
    "Standard_D2as_v4": {"Family": "Dav4", "vCPU": 2, "RAM": 8},
    "Standard_D4as_v4": {"Family": "Dav4", "vCPU": 4, "RAM": 16},
    "Standard_D8as_v4": {"Family": "Dav4", "vCPU": 8, "RAM": 32},
    "Standard_D16as_v4": {"Family": "Dav4", "vCPU": 16, "RAM": 64},
    "Standard_D32as_v4": {"Family": "Dav4", "vCPU": 32, "RAM": 128},
    
    "Standard_D2as_v5": {"Family": "Dav5", "vCPU": 2, "RAM": 8},
    "Standard_D4as_v5": {"Family": "Dav5", "vCPU": 4, "RAM": 16},
    "Standard_D8as_v5": {"Family": "Dav5", "vCPU": 8, "RAM": 32},
    "Standard_D16as_v5": {"Family": "Dav5", "vCPU": 16, "RAM": 64},
    "Standard_D32as_v5": {"Family": "Dav5", "vCPU": 32, "RAM": 128},

    # --- NOVO: Memory Optimized (Série E v3) ---
    "Standard_E2s_v3": {"Family": "Ev3", "vCPU": 2, "RAM": 16},
    "Standard_E4s_v3": {"Family": "Ev3", "vCPU": 4, "RAM": 32},
    "Standard_E8s_v3": {"Family": "Ev3", "vCPU": 8, "RAM": 64},
    "Standard_E16s_v3": {"Family": "Ev3", "vCPU": 16, "RAM": 128},
    "Standard_E32s_v3": {"Family": "Ev3", "vCPU": 32, "RAM": 256},
    "Standard_E48s_v3": {"Family": "Ev3", "vCPU": 48, "RAM": 384},
    "Standard_E64s_v3": {"Family": "Ev3", "vCPU": 64, "RAM": 432},

    # Memory Optimized (Série E v4)
    "Standard_E2s_v4": {"Family": "Ev4", "vCPU": 2, "RAM": 16},
    "Standard_E4s_v4": {"Family": "Ev4", "vCPU": 4, "RAM": 32},
    "Standard_E8s_v4": {"Family": "Ev4", "vCPU": 8, "RAM": 64},
    "Standard_E16s_v4": {"Family": "Ev4", "vCPU": 16, "RAM": 128},
    "Standard_E20s_v4": {"Family": "Ev4", "vCPU": 20, "RAM": 160},
    "Standard_E32s_v4": {"Family": "Ev4", "vCPU": 32, "RAM": 256},
    "Standard_E48s_v4": {"Family": "Ev4", "vCPU": 48, "RAM": 384},
    "Standard_E64s_v4": {"Family": "Ev4", "vCPU": 64, "RAM": 504},

    # Memory Optimized (Série E v5)
    "Standard_E2s_v5": {"Family": "Ev5", "vCPU": 2, "RAM": 16},
    "Standard_E4s_v5": {"Family": "Ev5", "vCPU": 4, "RAM": 32},
    "Standard_E8s_v5": {"Family": "Ev5", "vCPU": 8, "RAM": 64},
    "Standard_E16s_v5": {"Family": "Ev5", "vCPU": 16, "RAM": 128},
    "Standard_E20s_v5": {"Family": "Ev5", "vCPU": 20, "RAM": 160},
    "Standard_E32s_v5": {"Family": "Ev5", "vCPU": 32, "RAM": 256},

    # --- NOVO: Memory Optimized AMD (Série Ea v4 / Ea v5) ---
    "Standard_E2as_v4": {"Family": "Eav4", "vCPU": 2, "RAM": 16},
    "Standard_E4as_v4": {"Family": "Eav4", "vCPU": 4, "RAM": 32},
    "Standard_E8as_v4": {"Family": "Eav4", "vCPU": 8, "RAM": 64},
    "Standard_E16as_v4": {"Family": "Eav4", "vCPU": 16, "RAM": 128},
    
    "Standard_E2as_v5": {"Family": "Eav5", "vCPU": 2, "RAM": 16},
    "Standard_E4as_v5": {"Family": "Eav5", "vCPU": 4, "RAM": 32},
    "Standard_E8as_v5": {"Family": "Eav5", "vCPU": 8, "RAM": 64},
    "Standard_E16as_v5": {"Family": "Eav5", "vCPU": 16, "RAM": 128},

    # Compute Optimized (Série F v2)
    "Standard_F2s_v2": {"Family": "Fv2", "vCPU": 2, "RAM": 4},
    "Standard_F4s_v2": {"Family": "Fv2", "vCPU": 4, "RAM": 8},
    "Standard_F8s_v2": {"Family": "Fv2", "vCPU": 8, "RAM": 16},
    "Standard_F16s_v2": {"Family": "Fv2", "vCPU": 16, "RAM": 32},
    "Standard_F32s_v2": {"Family": "Fv2", "vCPU": 32, "RAM": 64},
    "Standard_F48s_v2": {"Family": "Fv2", "vCPU": 48, "RAM": 96},
    "Standard_F64s_v2": {"Family": "Fv2", "vCPU": 64, "RAM": 128},
}


FAMILY_INDEX = {}
for sku, info in SKU_CATALOG.items():
    fam = info["Family"]
    if fam not in FAMILY_INDEX: FAMILY_INDEX[fam] = []
    FAMILY_INDEX[fam].append(sku)
for fam in FAMILY_INDEX:
    FAMILY_INDEX[fam].sort(key=lambda x: SKU_CATALOG[x]["vCPU"])

def suggest_sku(current_sku, direction):
    if current_sku not in SKU_CATALOG: return "N/A"
    fam = SKU_CATALOG[current_sku]["Family"]
    sku_list = FAMILY_INDEX[fam]
    try:
        idx = sku_list.index(current_sku)
        if direction == "upgrade" and idx < len(sku_list) - 1: return sku_list[idx + 1]
        if direction == "downgrade" and idx > 0: return sku_list[idx - 1]
    except: pass
    return "Limite atingido"

from datetime import datetime, timedelta
import numpy as np

def get_metrics_p95(monitor_client, resource_id, ram_gb):
    end = datetime.utcnow()
    start = end - timedelta(days=LOOKBACK_DAYS)
    timespan = f"{start.isoformat()}Z/{end.isoformat()}Z"
    res = {"cpu_p95": 0, "mem_p95": 0}
    
    # Tentamos primeiro buscar o Percentile nativo do Azure. 
    # Se falhar ou não retornar dados, usamos Average com intervalo de 30min.
    
    from datetime import datetime, timedelta
    import numpy as np

def get_metrics_p95(monitor_client, resource_id, ram_gb):
    end = datetime.utcnow()
    start = end - timedelta(days=LOOKBACK_DAYS)
    timespan = f"{start.isoformat()}Z/{end.isoformat()}Z"
    res = {"cpu_p95": 0, "mem_p95": 0}
    
    def fetch_metric(metric_name, aggregation_type, interval):
        try:
            return monitor_client.metrics.list(
                resource_id, 
                timespan=timespan, 
                interval=interval, 
                metricnames=metric_name, 
                aggregation=aggregation_type
            )
        except:
            return None

    try:
        # --- PROCESSAMENTO CPU ---
        cpu_data = fetch_metric('Percentage CPU', 'Percentile', 'PT1H')
        cpu_vals = []
        if cpu_data and cpu_data.value:
            cpu_vals = [d.percentile for m in cpu_data.value for d in m.timeseries[0].data if d.percentile is not None]
        
        if not cpu_vals:
            cpu_data_avg = fetch_metric('Percentage CPU', 'Average', 'PT30M')
            if cpu_data_avg:
                cpu_vals = [d.average for m in cpu_data_avg.value for d in m.timeseries[0].data if d.average is not None]

        if cpu_vals:
            # Garante que CPU não seja negativo nem passe de 100%
            cpu_vals = [max(0, min(100, v)) for v in cpu_vals]
            res["cpu_p95"] = round(float(np.percentile(cpu_vals, 95)), 2)

        # --- PROCESSAMENTO MEMÓRIA ---
        mem_data = fetch_metric('Available Memory Bytes', 'Percentile', 'PT1H')
        mem_vals = []
        if mem_data and mem_data.value:
            mem_vals = [d.percentile for m in mem_data.value for d in m.timeseries[0].data if d.percentile is not None]
            
        if not mem_vals:
            mem_data_avg = fetch_metric('Available Memory Bytes', 'Average', 'PT30M')
            if mem_data_avg:
                mem_vals = [d.average for m in mem_data_avg.value for d in m.timeseries[0].data if d.average is not None]

        if mem_vals and ram_gb > 0:
            total_bytes = ram_gb * (1024**3)
            usage_list = []
            for v in mem_vals:
                # v = Memória disponível. 
                # Se v > total_bytes, o uso seria negativo. Usamos max(0, ...) para evitar isso.
                usage_percent = (1 - (v / total_bytes)) * 100
                # Trava entre 0 e 100
                usage_list.append(max(0, min(100, usage_percent)))
            
            if usage_list:
                res["mem_p95"] = round(float(np.percentile(usage_list, 95)), 2)

    except Exception as e:
        # print(f"Erro: {e}") # Útil para debug
        pass

    return res
    
 #def get_metrics_p95(monitor_client, resource_id, ram_gb):
 #   end = datetime.utcnow()
 #   start = end - timedelta(days=LOOKBACK_DAYS)
 #   timespan = f"{start.isoformat()}Z/{end.isoformat()}Z"
 #   res = {"cpu_p95": 0, "mem_p95": 0}
 #   try:
 #       cpu_data = monitor_client.metrics.list(resource_id, timespan=timespan, interval='PT1H', metricnames='Percentage CPU', aggregation='Average')
 #       cpu_vals = [d.average for m in cpu_data.value for d in m.timeseries[0].data if d.average is not None]
 #       if cpu_vals: res["cpu_p95"] = round(float(np.percentile(cpu_vals, 95)), 2)
 #       
 #       mem_data = monitor_client.metrics.list(resource_id, timespan=timespan, interval='PT1H', metricnames='Available Memory Bytes', aggregation='Average')
 #       mem_vals = [d.average for m in mem_data.value for d in m.timeseries[0].data if d.average is not None]
 #       if mem_vals and ram_gb > 0:
 #           total = ram_gb * (1024**3)
 #           usage_list = [(1 - (v / total)) * 100 for v in mem_vals]
 #           res["mem_p95"] = round(float(np.percentile(usage_list, 95)), 2)
 #   except: pass
 #   return res

# --- NOVA FUNÇÃO DE COLETA DE RI ---
def get_all_reservations(credential):
    ri_list = []
    print("🎟️  Coletando Reserved Instances do Tenant...")
    try:
        res_client = AzureReservationAPI(credential)
        # Lista todas as ordens de reserva
        orders = res_client.reservation_order.list()
        for order in orders:
            # Lista as reservas dentro de cada ordem
            reservations = res_client.reservation.list(order.name)
            for r in reservations:
                if r.properties.provisioning_state == "Succeeded":
                    ri_list.append({
                        "Sku": r.sku.name.lower().replace("_", ""),
                        "Name": r.name,
                        "Expiry": r.properties.expiry_date,
                        "State": r.properties.provisioning_state
                    })
    except Exception as e:
        print(f"⚠️  Aviso: Não foi possível ler RIs ({e})")
    return ri_list

def run_analysis():
    print("--- 🔐 Login via Browser ---")
    cred = InteractiveBrowserCredential()
    
    # Coleta RIs uma vez para todo o processo
    ri_data = get_all_reservations(cred)
    
    sub_client = SubscriptionClient(cred)
    final_report = []

    for sub in sub_client.subscriptions.list():
        print(f"\n📦 Sub: {sub.display_name}")
        compute_client = ComputeManagementClient(cred, sub.subscription_id)
        monitor_client = MonitorManagementClient(cred, sub.subscription_id)
        
        vms = list(compute_client.virtual_machines.list_all())
        for vm in vms:

            # Extrair Resource Group do ID da VM
            rg_name = vm.id.split("/")[4]
            
            # --- NOVIDADE: Captura do Status Real ---
            vm_status = "Unknown"
            try:
                # O expand='instanceView' é o que permite ver se está PowerState/running
                vm_detail = compute_client.virtual_machines.get(rg_name, vm.name, expand='instanceView')
                for status in vm_detail.instance_view.statuses:
                    if "PowerState" in status.code:
                        vm_status = status.display_status # Ex: "VM running" ou "VM deallocated"
            except: pass





            sku_original = vm.hardware_profile.vm_size
            info = SKU_CATALOG.get(sku_original, {"Family": "OUTRO", "vCPU": 0, "RAM": 1})
            metrics = get_metrics_p95(monitor_client, vm.id, info["RAM"])
            
            # Match de RI (Lógica igual ao PowerShell)
            sku_clean = sku_original.lower().replace("_", "").replace("standard", "")
            vm_has_ri = "Não"
            for ri in ri_data:
                if sku_clean in ri["Sku"] or ri["Sku"] in sku_clean:
                    vm_has_ri = f"Sim ({ri['Name']})"
                    break

            rec, sug = "OTIMIZADO", ""
            if metrics["cpu_p95"] >= CPU_UPGRADE_P95 or metrics["mem_p95"] >= MEM_UPGRADE_P95:
                rec, sug = "UPGRADE", suggest_sku(sku_original, "upgrade")
            elif metrics["cpu_p95"] <= CPU_DOWNGRADE_P95 and metrics["mem_p95"] <= MEM_DOWNGRADE_P95:
                rec, sug = "DOWNGRADE", suggest_sku(sku_original, "downgrade")
                reason = f"Subutilização Total (CPU: {metrics['cpu_p95']}% e Mem: {metrics['mem_p95']}%)"
            #elif 0 < metrics["cpu_p95"] <= CPU_DOWNGRADE_P95:
            #    rec, sug = "DOWNGRADE", suggest_sku(sku_original, "downgrade")

            final_report.append({
                "Subscription": sub.display_name,
                "SubscriptionID": sub.subscription_id,
                "VmName": vm.name,
                "Region": vm.location,
                "CurrentSku": sku_original,
                "CPU_P95": metrics["cpu_p95"],
                "MEM_P95": metrics["mem_p95"],
                "HasRI": vm_has_ri, 
                "Recommendation": rec,
                "SuggestedSku": sug,
                "Status": vm_status
            })
            print(f"  ✅ {vm.name} processada.")

    df = pd.DataFrame(final_report)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    df.to_csv(f"finops_report_final_{timestamp}.csv", index=False, encoding='utf-8-sig')
    print(f"\n🏁 Concluído! Relatório gerado com sucesso. finops_report_final_{timestamp}.csv")

if __name__ == "__main__":
    run_analysis()