"""
Menú interactivo para ejecutar scripts de monitorización y utilidades del sistema.
Permite seleccionar y lanzar fácilmente los principales scripts de monitorización y gestión.
Incluye opción para limpiar la colección aggregated_data y submenú para gestión de contenedores Docker.
No muestra contraseñas en claro y usa variables de entorno para la configuración.
Incluye opción para ver el estado de los contenedores Docker directamente.
"""

import os
import sys
import subprocess
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[3]
UTILS_DIR = ROOT_DIR / "Estructura" / "shared" / "utils"

MENU_OPTIONS = [
    {"key": "1",  "desc": "📊 Estadísticas rápidas de MongoDB",                      "script": "mongodb_stats.py"},
    {"key": "2",  "desc": "🧹 Limpiar colección aggregated_data (solo borra datos agregados)", "script": "cleared_aggregated_data.py"},
    {"key": "3",  "desc": "🧹 Limpiar colección raw_messages (solo borra mensajes brutos)",    "script": "cleared_raw_messages.py"},
    {"key": "4",  "desc": "🔄 Parar/levantar contenedores Docker (submenú)",          "submenu": True},
    {"key": "5",  "desc": "🔍 Ver estado de los contenedores Docker",                 "status": True},
    {"key": "6",  "desc": "📜 Ver logs de un contenedor Docker",                      "logs": True},
    {"key": "7",  "desc": "⏱ Monitor de procesamiento en tiempo real",               "script": "monitor_processing.py"},
    {"key": "8",  "desc": "🔎 Análisis de patrones en raw_messages",                  "script": "check_messages.py"},
    {"key": "9",  "desc": "📊 Análisis de distribución de tipos agregados",           "script": "analyze_distribution.py"},
    {"key": "10", "desc": "🔌 Test de conexión a MongoDB",                            "script": "test_connection.py"},
    {"key": "11", "desc": "🔄 Resetear y testear el sistema",                         "script": "reset_and_test.py"},
    # NUEVAS
    {"key": "12", "desc": "🧰 Herramientas Mongo (aggregated_data / raw_messages)",   "script": "mongo_aggregated_tools.py"},
    {"key": "13", "desc": "📈 Métricas SQL Persister (Mongo ➜ Postgres)",             "script": "sql_persister_metrics.py"},
    {"key": "14", "desc": "📜 Ver logs del contenedor sql-persister",                 "sql_logs": True, "container": "sql-persister"},
    {"key": "q",  "desc": "❌ Salir",                                                 "script": None}
]

def show_docker_logs():
    from docker_management_menu import get_services_from_compose, DOCKER_FILES
    kafka_services = get_services_from_compose(DOCKER_FILES[0])
    service_services = get_services_from_compose(DOCKER_FILES[1])
    all_services = kafka_services + service_services
    print("\nContenedores disponibles para logs:")
    for idx, (name, container) in enumerate(all_services, 1):
        print(f"  {idx}. {name} ({container})")
    idx = input("Selecciona el número del contenedor para ver logs: ").strip()
    try:
        idx = int(idx)
        if 1 <= idx <= len(all_services):
            container = all_services[idx-1][1]
            print(f"\n--- Mostrando logs de {container} ---\nPresiona Ctrl+C para salir.\n")
            try:
                subprocess.run(["docker", "logs", "-f", container])
            except KeyboardInterrupt:
                print("\n⏪ Saliendo de los logs...")
        else:
            print("Opción inválida.")
    except ValueError:
        print("Opción inválida.")

def show_sql_persister_logs():
    # Comprueba si existe el contenedor
    try:
        res = subprocess.run(["docker", "ps", "-a", "--format", "{{.Names}}"],
                             capture_output=True, text=True)
        if res.returncode != 0:
            print("⚠️  No se pudieron listar contenedores Docker.")
        names = (res.stdout or "").splitlines()
        if "sql-persister" not in names:
            print("❌ El contenedor 'sql-persister' no existe todavía. Inícialo con docker compose.")
            return
    except Exception:
        pass

    print("\n--- Mostrando logs de sql-persister ---\nPresiona Ctrl+C para salir.\n")
    try:
        subprocess.run(["docker", "logs", "-f", "sql-persister"])
    except KeyboardInterrupt:
        print("\n⏪ Saliendo de los logs...")

def run_script(script_name):
    script_path = UTILS_DIR / script_name
    if not script_path.exists():
        print(f"❌ El script {script_name} no existe en {script_path}")
        return
    print(f"\n🔄 Ejecutando: {script_name}\n{'-'*80}")
    try:
        subprocess.run([sys.executable, str(script_path)], check=False)
    except KeyboardInterrupt:
        print("\n⏪ Volviendo al menú...")

def run_docker_management_menu():
    docker_menu_script = UTILS_DIR / "docker_management_menu.py"
    try:
        subprocess.run([sys.executable, str(docker_menu_script)], check=False)
    except KeyboardInterrupt:
        print("\n⏪ Volviendo al menú...")

def show_docker_status():
    docker_status_script = UTILS_DIR / "docker_management_menu.py"
    try:
        subprocess.run([sys.executable, str(docker_status_script), "--status"], check=False)
    except KeyboardInterrupt:
        print("\n⏪ Volviendo al menú...")

def main_menu():
    while True:
        print("\n" + "="*80)
        print("        MENÚ DE UTILIDADES Y MONITORIZACIÓN DATA-PROCESSOR")
        print("="*80)
        for opt in MENU_OPTIONS:
            print(f"{opt['key']}. {opt['desc']}")
        choice = input("\nElige una opción: ").strip()
        if choice == "q":
            print("¡Hasta luego!")
            break
        selected = next((o for o in MENU_OPTIONS if o["key"] == choice), None)
        if not selected:
            print("Opción no válida.")
            continue

        if selected.get("submenu"):
            run_docker_management_menu()
        elif selected.get("status"):
            show_docker_status()
        elif selected.get("logs"):
            show_docker_logs()
        elif selected.get("sql_logs"):
            show_sql_persister_logs()
        elif selected.get("script"):
            run_script(selected.get("script"))
        else:
            print("Opción no reconocida.")

        input("\nPresiona ENTER para volver al menú...")

if __name__ == "__main__":
    main_menu()
