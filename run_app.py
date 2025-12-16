"""
Launcher script for PyInstaller-packaged Streamlit app.

This script:
1. Detects if running from a PyInstaller bundle (frozen) or development
2. Sets up the correct working directory
3. Launches Streamlit as a subprocess
4. Opens the browser automatically
5. Handles graceful shutdown
"""

import os
import sys
import subprocess
import webbrowser
import time
import signal
import atexit

# Configuration
PORT = 8501
HOST = "localhost"
STREAMLIT_APP = "streamlit_app.py"


def get_base_path():
    """Get the base path for resources (handles PyInstaller frozen state)."""
    if getattr(sys, 'frozen', False):
        # Running as PyInstaller bundle
        return sys._MEIPASS
    else:
        # Running in development
        return os.path.dirname(os.path.abspath(__file__))


def find_streamlit_executable():
    """Find the streamlit executable or module path."""
    if getattr(sys, 'frozen', False):
        # In frozen mode, use the bundled Python to run streamlit as module
        return [sys.executable, "-m", "streamlit"]
    else:
        # In development, try to find streamlit in PATH or use module
        return [sys.executable, "-m", "streamlit"]


def open_browser_with_retry(url, max_retries=10, delay=0.5):
    """Open browser after checking if server is ready."""
    import urllib.request
    import urllib.error
    
    for i in range(max_retries):
        try:
            urllib.request.urlopen(url, timeout=1)
            webbrowser.open(url)
            print(f"✓ Navigateur ouvert sur {url}")
            return True
        except (urllib.error.URLError, urllib.error.HTTPError):
            time.sleep(delay)
    
    # Open anyway after retries
    webbrowser.open(url)
    print(f"✓ Navigateur ouvert sur {url} (serveur peut-être pas encore prêt)")
    return True


def main():
    """Main entry point for the packaged application."""
    base_path = get_base_path()
    url = f"http://{HOST}:{PORT}"
    
    print("=" * 50)
    print("  Parquet → CSV Converter")
    print("=" * 50)
    print(f"📁 Chemin de base: {base_path}")
    print(f"🌐 URL: {url}")
    print("=" * 50)
    
    # Change to base path so Streamlit can find all resources
    original_cwd = os.getcwd()
    os.chdir(base_path)
    print(f"📂 Répertoire de travail: {os.getcwd()}")
    
    # Check if streamlit_app.py exists
    if not os.path.exists(STREAMLIT_APP):
        print(f"❌ Erreur: {STREAMLIT_APP} introuvable dans {base_path}")
        input("Appuyez sur Entrée pour fermer...")
        sys.exit(1)
    
    # Build the streamlit command
    streamlit_cmd = find_streamlit_executable()
    cmd = streamlit_cmd + [
        "run",
        STREAMLIT_APP,
        f"--server.port={PORT}",
        "--server.headless=true",
        "--server.address=localhost",
        "--browser.gatherUsageStats=false",
    ]
    
    print(f"🚀 Lancement: {' '.join(cmd)}")
    print("-" * 50)
    
    # Start Streamlit process
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
        )
    except FileNotFoundError as e:
        print(f"❌ Erreur: Impossible de lancer Streamlit: {e}")
        input("Appuyez sur Entrée pour fermer...")
        sys.exit(1)
    
    # Register cleanup on exit
    def cleanup():
        if process.poll() is None:
            print("\n🛑 Arrêt du serveur Streamlit...")
            if sys.platform == "win32":
                process.terminate()
            else:
                process.send_signal(signal.SIGTERM)
            process.wait(timeout=5)
    
    atexit.register(cleanup)
    
    # Open browser in a separate thread after a delay
    import threading
    browser_thread = threading.Thread(
        target=open_browser_with_retry,
        args=(url,),
        daemon=True
    )
    browser_thread.start()
    
    # Stream output from Streamlit
    try:
        print("\n📋 Logs Streamlit:")
        print("-" * 50)
        for line in process.stdout:
            print(line, end='')
    except KeyboardInterrupt:
        print("\n\n⚠️ Interruption reçue (Ctrl+C)")
    finally:
        cleanup()
        os.chdir(original_cwd)
    
    print("\n✓ Application fermée.")
    return process.returncode or 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:
        print(f"\n❌ Erreur fatale: {e}")
        import traceback
        traceback.print_exc()
        input("\nAppuyez sur Entrée pour fermer...")
        sys.exit(1)
