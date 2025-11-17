"""
Menú interactivo para ejecutar scripts de monitorización y utilidades del sistema.
Permite seleccionar y lanzar fácilmente los principales scripts de monitorización y gestión.
No muestra contraseñas en claro y usa variables de entorno para la configuración.
"""

import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent.parent
SCRIPTS = [
    ("Estadísticas rápidas de MongoDB", "mongodb_stats.py"),
    ("Monitor de procesamiento en tiempo real", "monitor_processing.py"),
    ("Análisis de patrones en raw_messages", "check_messages.py"),
    ("Análisis de distribución de tipos agregados", "analyze_distribution.py"),
    ("Test de conexión a MongoDB", "test_connection.py"),
    ("Resetear y testear el sistema", "reset_and_test.py"),
]

def print_menu():
    print("=" * 80)
    print("📋 MENÚ DE MONITORIZACIÓN Y UTILIDADES")
    print("=" * 80)
    for idx, (desc, script) in enumerate(SCRIPTS, 1):
        print(f"  {idx}. {desc} ({script})")
    print("  0. Salir")
    print("=" * 80)

def run_script(script_name):
    script_path = ROOT_DIR / "Estructura" / "shared" / "utils" / script_name
    if not script_path.exists():
        print(f"❌ El script {script_name} no existe en {script_path}")
        return
    print(f"\n🔄 Ejecutando: {script_name}\n{'-'*80}")
    os.system(f"{sys.executable} \"{script_path}\"")

def main():
    while True:
        print_menu()
        choice = input("Selecciona una opción (número): ").strip()
        if choice == "0":
            print("\n👋 Saliendo del menú.")
            break
        try:
            idx = int(choice)
            if 1 <= idx <= len(SCRIPTS):
                run_script(SCRIPTS[idx-1][1])
            else:
                print("⚠️  Opción inválida.")
        except ValueError:
            print("⚠️  Ingresa un número válido.")

if __name__ == "__main__":
    main()