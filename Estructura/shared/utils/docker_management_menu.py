import os
import subprocess
import sys
import time
from pathlib import Path
import yaml

ROOT_DIR = Path(__file__).resolve().parents[3]
DOCKER_FILES = [
    ROOT_DIR / "docker-compose-kafka.yml",
    ROOT_DIR / "docker-compose-services.yml"
]

def get_service_container_list():
    service_container_list = []
    for file in DOCKER_FILES:
        if file.exists():
            with open(file, "r", encoding="utf-8") as f:
                try:
                    data = yaml.safe_load(f)
                    services = data.get("services", {})
                    for svc, cfg in services.items():
                        container_name = cfg.get("container_name", svc)
                        service_container_list.append((svc, container_name))
                except Exception as e:
                    print(f"Error leyendo {file}: {e}")
    return service_container_list

def get_container_status(container_name):
    try:
        result = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Status}}", container_name],
            capture_output=True, text=True, timeout=5
        )
        status = result.stdout.strip()
        if status == "running":
            icon = "🟢"
        elif status == "exited":
            icon = "🔴"
        elif status == "restarting":
            icon = "🟡"
        elif status == "paused":
            icon = "⏸"
        elif status == "dead":
            icon = "💀"
        elif status == "created":
            icon = "⚪"
        elif status == "":
            icon = "⚪"
            status = "not found"
        else:
            icon = "⚪"
        return f"{icon} {status:<10}"
    except Exception:
        return "⚪ not found"

def get_services_from_compose(compose_file):
    services = []
    if compose_file.exists():
        with open(compose_file, "r", encoding="utf-8") as f:
            try:
                data = yaml.safe_load(f)
                for svc, cfg in data.get("services", {}).items():
                    container_name = cfg.get("container_name", svc)
                    services.append((svc, container_name))
            except Exception as e:
                print(f"Error leyendo {compose_file}: {e}")
    return services

def show_status_all():
    kafka_services = get_services_from_compose(DOCKER_FILES[0])
    service_services = get_services_from_compose(DOCKER_FILES[1])
    all_services = kafka_services + service_services
    print("\nEstado actual de los contenedores:")
    print(f"{'Servicio':<20} {'Contenedor':<25} {'Estado'} {'   Sugerencia de acción(ejecución)'}")
    print("-" * 90)
    for service, container in all_services:
        status = get_container_status(container)
        if "not found" in status:
            suggestion = f"👉 docker-compose up -d {service}"
        else:
            suggestion = ""
        print(f"{service:<20} {container:<25} {status:<15} {suggestion}")

def stop_one(service_name):
    service_container_list = get_service_container_list()
    found = False
    for svc, container in service_container_list:
        if svc == service_name:
            found = True
            status = get_container_status(container)
            if "running" in status:
                subprocess.run(["docker", "stop", container])
                print(f"✅ Contenedor {container} parado.")
            else:
                print(f"⚪ {container} ya está parado o no existe.")
            if "not found" in status:
                print(f"👉 Usa: docker-compose up -d {service_name}   (en el compose correspondiente)")
    if not found:
        print(f"⚪ Servicio '{service_name}' no reconocido.")

def start_one(service_name):
    service_container_list = get_service_container_list()
    found = False
    for svc, container in service_container_list:
        if svc == service_name:
            found = True
            status = get_container_status(container)
            if "not found" in status:
                print(f"⚪ El contenedor {container} no existe. Creando con docker-compose...")
                # Detecta el archivo docker-compose correcto
                compose_file = None
                for file in DOCKER_FILES:
                    services = [s for s, _ in get_services_from_compose(file)]
                    if service_name in services:
                        compose_file = file
                        break
                if compose_file:
                    subprocess.run(["docker-compose", "-f", str(compose_file), "up", "-d", service_name])
                    print(f"✅ Contenedor {container} creado y levantado.")
                else:
                    print(f"❌ No se encontró el archivo docker-compose para {service_name}.")
            elif "running" in status:
                print(f"🟢 {container} ya está levantado.")
            else:
                subprocess.run(["docker", "start", container])
                print(f"✅ Contenedor {container} levantado.")
    if not found:
        print(f"⚪ Servicio '{service_name}' no reconocido.")


def build_one(service_name):
    print(f"\n🔨 Reconstruyendo contenedor: {service_name}")
    subprocess.run(["docker-compose", "build", service_name])
    print(f"✅ Build/reconstrucción de {service_name} completada.")

def stop_group(group_services):
    service_container_list = get_service_container_list()
    print("\n🔄 Parando contenedores:")
    for name in group_services:
        for svc, container in service_container_list:
            if svc == name:
                print(f"  - {svc} ({container})")
    for name in group_services:
        stop_one(name)
    print("✅ Contenedores parados.")

def start_group(group_services, kafka=False):
    service_container_list = get_service_container_list()
    print("\n🔄 Levantando contenedores:")
    for name in group_services:
        for svc, container in service_container_list:
            if svc == name:
                print(f"  - {svc} ({container})")
    for name in group_services:
        start_one(name)
    print("✅ Contenedores levantados.")
    if kafka and "random_generator" in group_services:
        print("ℹ️ El contenedor random_generator se parará automáticamente en 2 minutos.")
        time.sleep(120)
        stop_one("random_generator")
        print("⏱ random_generator parado tras 2 minutos.")

def docker_menu():
    kafka_services = get_services_from_compose(DOCKER_FILES[0])
    service_services = get_services_from_compose(DOCKER_FILES[1])
    all_services = kafka_services + service_services

    while True:
        print("\n--- Gestión de contenedores Docker ---")
        print("0. Ver estado de todos los contenedores")
        print("1. Parar todos los contenedores")
        print("2. Levantar todos los contenedores")
        print("3. Parar un contenedor individual")
        print("4. Levantar un contenedor individual")
        print("5. Build/reconstruir un contenedor individual")
        print("6. Ver logs de un contenedor individual")
        print("7. Parar solo Kafka/generación de datos")
        print("8. Levantar solo Kafka/generación de datos (random_generator se parará en 2 min)")
        print("9. Parar solo servicios")
        print("10. Levantar solo servicios")
        print("q. Volver al menú principal")
        choice = input("Elige una opción: ").strip()
        if choice == "0":
            show_status_all()
        elif choice == "1":
            stop_group([svc for svc, _ in all_services])
        elif choice == "2":
            start_group([svc for svc, _ in all_services])
        elif choice == "3":
            print("\nContenedores disponibles:")
            for idx, (name, container) in enumerate(all_services, 1):
                print(f"  {idx}. {name} ({container})")
            idx = input("Selecciona el número del contenedor a parar: ").strip()
            try:
                idx = int(idx)
                if 1 <= idx <= len(all_services):
                    stop_one(all_services[idx-1][0])
                else:
                    print("Opción inválida.")
            except ValueError:
                print("Opción inválida.")
        elif choice == "4":
            print("\nContenedores disponibles:")
            for idx, (name, container) in enumerate(all_services, 1):
                print(f"  {idx}. {name} ({container})")
            idx = input("Selecciona el número del contenedor a levantar: ").strip()
            try:
                idx = int(idx)
                if 1 <= idx <= len(all_services):
                    start_one(all_services[idx-1][0])
                else:
                    print("Opción inválida.")
            except ValueError:
                print("Opción inválida.")
        elif choice == "5":
            print("\nContenedores disponibles:")
            for idx, (name, container) in enumerate(all_services, 1):
                print(f"  {idx}. {name} ({container})")
            idx = input("Selecciona el número del contenedor a reconstruir: ").strip()
            try:
                idx = int(idx)
                if 1 <= idx <= len(all_services):
                    build_one(all_services[idx-1][0])
                else:
                    print("Opción inválida.")
            except ValueError:
                print("Opción inválida.")
        elif choice == "6":
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
        elif choice == "7":
            stop_group([svc for svc, _ in kafka_services])
        elif choice == "8":
            start_group([svc for svc, _ in kafka_services], kafka=True)
        elif choice == "9":
            stop_group([svc for svc, _ in service_services])
        elif choice == "10":
            start_group([svc for svc, _ in service_services])
        elif choice == "q":
            break
        else:
            print("Opción no válida.")
        input("\nPresiona ENTER para continuar...")

if __name__ == "__main__":
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "--status":
            show_status_all()
        else:
            docker_menu()
    except KeyboardInterrupt:
        print("\n⏪ Volviendo al menú principal...")