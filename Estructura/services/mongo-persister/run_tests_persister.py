"""
Script para ejecutar tests del MONGO PERSISTER con output bonito y reporte HTML
Uso: python run_tests_persister.py [opciones]
"""
import sys
import os
import subprocess
import argparse
from pathlib import Path


class Colors:
    """Colores ANSI para terminal"""
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BLUE = '\033[94m'
    MAGENTA = '\033[95m'
    BOLD = '\033[1m'
    END = '\033[0m'


def print_header(text: str):
    """Imprime header con formato"""
    print()
    print(f"{Colors.CYAN}{Colors.BOLD}{'=' * 70}{Colors.END}")
    print(f"{Colors.CYAN}{Colors.BOLD}  {text}{Colors.END}")
    print(f"{Colors.CYAN}{Colors.BOLD}{'=' * 70}{Colors.END}")
    print()


def print_success(text: str):
    """Imprime mensaje de éxito"""
    print(f"{Colors.GREEN}[✓] {text}{Colors.END}")


def print_error(text: str):
    """Imprime mensaje de error"""
    print(f"{Colors.RED}[✗] {text}{Colors.END}")


def print_info(text: str):
    """Imprime mensaje informativo"""
    print(f"{Colors.YELLOW}[*] {text}{Colors.END}")


def print_command(text: str):
    """Imprime comando a ejecutar"""
    print(f"{Colors.BLUE}[>] {text}{Colors.END}")


def print_menu():
    """Imprime menú de opciones disponibles"""
    print()
    print(f"{Colors.MAGENTA}{Colors.BOLD}📋 OPCIONES RÁPIDAS:{Colors.END}")
    print()
    print(f"{Colors.CYAN}  Básicos:{Colors.END}")
    print(f"    python run_tests_persister.py                    → Tests básicos")
    print(f"    python run_tests_persister.py -c                 → Con coverage")
    print(f"    python run_tests_persister.py -c -o              → Coverage + abrir HTML")
    print()
    print(f"{Colors.CYAN}  Filtros:{Colors.END}")
    print(f"    python run_tests_persister.py -m unit            → Solo tests unitarios")
    print(f"    python run_tests_persister.py -m integration     → Solo tests integración")
    print(f"    python run_tests_persister.py -m performance     → Solo tests performance")
    print(f"    python run_tests_persister.py -m persistence     → Solo tests persistencia")
    print(f"    python run_tests_persister.py -f tests/test_config.py  → Archivo específico")
    print()
    print(f"{Colors.CYAN}  Debug:{Colors.END}")
    print(f"    python run_tests_persister.py -v                 → Ver prints de tests")
    print(f"    python run_tests_persister.py -d 10              → Top 10 tests lentos")
    print(f"    python run_tests_persister.py -x                 → Parar en primer fallo")
    print()
    print(f"{Colors.CYAN}  Combinaciones útiles:{Colors.END}")
    print(f"    python run_tests_persister.py -m unit -c -o      → Tests unitarios + coverage + HTML")
    print(f"    python run_tests_persister.py -m integration -v  → Tests integración con prints")
    print(f"    python run_tests_persister.py -f tests/test_persister.py -v -x  → Un archivo con debug")
    print()
    print(f"{Colors.CYAN}  Utilidades:{Colors.END}")
    print(f"    python run_tests_persister.py --clean            → Limpiar archivos generados")
    print(f"    python run_tests_persister.py --open-only        → Solo abrir último reporte")
    print(f"    python run_tests_persister.py --list-markers     → Listar markers disponibles")
    print(f"    python run_tests_persister.py --help             → Ver ayuda completa")
    print()
    print(f"{Colors.MAGENTA}{'─' * 70}{Colors.END}")
    print()


def check_venv():
    """Verifica si estamos en un virtualenv"""
    return hasattr(sys, 'real_prefix') or (
        hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix
    )


def list_markers():
    """Lista todos los markers disponibles"""
    print_info("Markers disponibles:")
    print()
    markers = {
        'unit': 'Tests unitarios (no requieren servicios externos)',
        'integration': 'Tests de integración (requieren MongoDB/Kafka reales)',
        'slow': 'Tests lentos (> 1 segundo)',
        'persistence': 'Tests de persistencia en MongoDB',
        'performance': 'Tests de rendimiento y benchmarking'
    }
    
    for marker, description in markers.items():
        print(f"  {Colors.CYAN}{marker:15}{Colors.END} → {description}")
    
    print()
    print(f"{Colors.YELLOW}Uso:{Colors.END} python run_tests_persister.py -m <marker>")
    print()


def run_tests(args):
    """Ejecuta los tests con pytest"""
    # Verificar que pytest está instalado
    try:
        import pytest
    except ImportError:
        print_error("pytest no está instalado")
        print_info("Instala con: pip install -r requirements-dev.txt")
        return 1
    
    # Construir comando pytest
    pytest_args = ['tests/', '-v', '--emoji']
    
    # Añadir coverage si se solicita
    if args.coverage:
        pytest_args.extend([
            '--cov=src',
            '--cov-report=html',
            '--cov-report=term-missing'
        ])
        
        # ✅ AÑADIR fail-under SOLO si NO es performance
        if args.marker != 'performance':
            pytest_args.append('--cov-fail-under=50')
        else:
            print_info("⚠️  Tests de performance: umbral de coverage deshabilitado")
            print()
    
    # Añadir marker si se especifica
    if args.marker:
        pytest_args.extend(['-m', args.marker])
    
    # Añadir verbose extra si se solicita
    if args.verbose:
        pytest_args.append('-s')
    
    # Parar en primer fallo si se solicita
    if args.fail_fast:
        pytest_args.append('-x')
    
    # Añadir durations si se solicita
    if args.durations:
        pytest_args.extend(['--durations', str(args.durations)])
    
    # Añadir archivo específico si se especifica
    if args.file:
        pytest_args[0] = args.file
    
    # Ejecutar solo tests que fallaron la última vez
    if args.failed:
        pytest_args.append('--lf')
        print_info("Ejecutando solo tests que fallaron en la última ejecución")
    
    # Ejecutar tests en paralelo si se solicita
    if args.parallel:
        pytest_args.extend(['-n', str(args.parallel)])
        print_info(f"Ejecutando tests en {args.parallel} procesos paralelos")
    
    print_command(f"pytest {' '.join(pytest_args)}")
    print()
    
    # Ejecutar pytest
    exit_code = pytest.main(pytest_args)
    
    return exit_code

def open_report():
    """Abre el reporte HTML de coverage"""
    report_path = Path('htmlcov/index.html')
    
    if not report_path.exists():
        print_error("No se encontró el reporte HTML")
        print_info("Ejecuta primero con: python run_tests_persister.py -c")
        return False
    
    print_info(f"Abriendo reporte: {report_path}")
    
    # Abrir según el sistema operativo
    try:
        if sys.platform == 'win32':
            os.startfile(report_path)
        elif sys.platform == 'darwin':  # macOS
            subprocess.run(['open', str(report_path)])
        else:  # Linux
            subprocess.run(['xdg-open', str(report_path)])
        return True
    except Exception as e:
        print_error(f"No se pudo abrir el reporte: {e}")
        print_info(f"Abre manualmente: {report_path}")
        return False


def clean_test_artifacts():
    """Limpia archivos generados por tests"""
    import shutil
    
    artifacts = [
        '.coverage',
        'htmlcov',
        '.pytest_cache',
        'tests/__pycache__',
        'src/__pycache__'
    ]
    
    print_info("Limpiando archivos de tests...")
    print()
    
    cleaned_count = 0
    for artifact in artifacts:
        path = Path(artifact)
        if path.exists():
            try:
                if path.is_file():
                    path.unlink()
                    print_success(f"Eliminado: {artifact}")
                else:
                    shutil.rmtree(path)
                    print_success(f"Eliminado: {artifact}/")
                cleaned_count += 1
            except Exception as e:
                print_error(f"No se pudo eliminar {artifact}: {e}")
    
    print()
    if cleaned_count > 0:
        print_success(f"Limpieza completada ({cleaned_count} items eliminados)")
    else:
        print_info("No había archivos para limpiar")


def main():
    """Función principal"""
    parser = argparse.ArgumentParser(
        description='Ejecutar tests del MONGO PERSISTER',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  
  BÁSICOS:
    python run_tests_persister.py                    # Tests básicos
    python run_tests_persister.py -c                 # Con coverage
    python run_tests_persister.py -c -o              # Coverage + abrir HTML
  
  FILTROS:
    python run_tests_persister.py -m unit            # Solo tests unitarios
    python run_tests_persister.py -m integration     # Solo tests integración
    python run_tests_persister.py -m performance     # Solo performance
    python run_tests_persister.py -f tests/test_config.py  # Test específico
  
  DEBUG:
    python run_tests_persister.py -v                 # Ver prints
    python run_tests_persister.py -d 10              # Top 10 lentos
    python run_tests_persister.py -x                 # Parar en primer fallo
    python run_tests_persister.py --failed           # Solo tests que fallaron
  
  PARALELO:
    python run_tests_persister.py -n 4               # 4 procesos paralelos
  
  COMBINACIONES:
    python run_tests_persister.py -m unit -c -o      # Unitarios + coverage + HTML
    python run_tests_persister.py -m integration -v  # Integración con prints
  
  UTILIDADES:
    python run_tests_persister.py --clean            # Limpiar archivos
    python run_tests_persister.py --open-only        # Solo abrir reporte
    python run_tests_persister.py --list-markers     # Listar markers
        """
    )
    
    parser.add_argument(
        '--coverage', '-c',
        action='store_true',
        help='Generar reporte de coverage'
    )
    
    parser.add_argument(
        '--open', '-o',
        action='store_true',
        help='Abrir reporte HTML después de ejecutar'
    )
    
    parser.add_argument(
        '--marker', '-m',
        type=str,
        choices=['unit', 'integration', 'slow', 'persistence', 'performance'],
        help='Ejecutar solo tests con marker específico'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Mostrar prints de los tests'
    )
    
    parser.add_argument(
        '--fail-fast', '-x',
        action='store_true',
        help='Parar en el primer test que falle'
    )
    
    parser.add_argument(
        '--durations', '-d',
        type=int,
        metavar='N',
        help='Mostrar los N tests más lentos'
    )
    
    parser.add_argument(
        '--file', '-f',
        type=str,
        help='Ejecutar solo un archivo de tests específico'
    )
    
    parser.add_argument(
        '--failed',
        action='store_true',
        help='Ejecutar solo los tests que fallaron en la última ejecución'
    )
    
    parser.add_argument(
        '--parallel', '-n',
        type=int,
        metavar='N',
        help='Ejecutar tests en N procesos paralelos (requiere pytest-xdist)'
    )
    
    parser.add_argument(
        '--clean',
        action='store_true',
        help='Limpiar archivos generados por tests'
    )
    
    parser.add_argument(
        '--open-only',
        action='store_true',
        help='Solo abrir el último reporte (sin ejecutar tests)'
    )
    
    parser.add_argument(
        '--list-markers',
        action='store_true',
        help='Listar todos los markers disponibles'
    )
    
    args = parser.parse_args()
    
    # Mostrar header principal
    print_header("🧪 MONGO PERSISTER - TEST SUITE")
    
    # Verificar virtualenv
    if check_venv():
        print_success("Entorno virtual activado")
    else:
        print_info("No se detectó entorno virtual (usando Python global)")
    
    # Acción: listar markers
    if args.list_markers:
        print()
        list_markers()
        print(f"{Colors.CYAN}{'=' * 70}{Colors.END}")
        print()
        return 0
    
    # Acción: limpiar
    if args.clean:
        print()
        clean_test_artifacts()
        print()
        print(f"{Colors.CYAN}{'=' * 70}{Colors.END}")
        print()
        return 0
    
    # Acción: solo abrir reporte
    if args.open_only:
        print()
        if open_report():
            print()
            print(f"{Colors.CYAN}{'=' * 70}{Colors.END}")
            print()
            return 0
        else:
            return 1
    
    # Mostrar menú de opciones ANTES de ejecutar
    print_menu()
    
    # Ejecutar tests
    print_info("Ejecutando tests...")
    print()
    
    exit_code = run_tests(args)
    
    # Mostrar resultados
    print()
    print_header("📊 RESULTADOS")
    
    if exit_code == 0:
        print_success("TODOS LOS TESTS PASARON ✨")
        print()
        
        if args.coverage:
            print_info("Reporte HTML generado en: htmlcov/index.html")
            print()
            
            # Abrir reporte si se solicitó
            if args.open:
                open_report()
            else:
                # Preguntar si quiere abrir
                try:
                    response = input(f"{Colors.YELLOW}¿Abrir reporte HTML? (s/n): {Colors.END}")
                    if response.lower() in ['s', 'si', 'yes', 'y']:
                        open_report()
                except (KeyboardInterrupt, EOFError):
                    print()
                    print_info("Cancelado")
    else:
        print_error("ALGUNOS TESTS FALLARON ❌")
        print()
        print_info("Revisa el output arriba para detalles")
        print_info("Tip: Usa -v para ver prints de los tests")
        print_info("Tip: Usa --failed para ejecutar solo los tests que fallaron")
    
    print()
    print(f"{Colors.CYAN}{'=' * 70}{Colors.END}")
    print()
    
    return exit_code


if __name__ == '__main__':
    sys.exit(main())