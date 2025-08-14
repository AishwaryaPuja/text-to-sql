import logging
import atexit
from flask import Flask
from flask_cors import CORS

from app.config import Config
from app.routes import routes as main_blueprint
from app.utils import cleanup_session
import app.utils as utils

def create_app(config_class=Config):
    app = Flask(__name__ ,template_folder="app/templates")
    app.config.from_object(config_class)
    CORS(app)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    app.register_blueprint(main_blueprint)

    def cleanup_all_active_sessions():
        logging.info("Application is shutting down. Cleaning up all active sessions...")
        active_uuids = list(utils.SESSIONS_DB.keys())
        if not active_uuids:
            logging.info("No active sessions were found to clean up.")
            return
        for uuid in active_uuids:
            cleanup_session(uuid)
        logging.info(f"Successfully cleaned up resources for {len(active_uuids)} session(s).")

    atexit.register(cleanup_all_active_sessions)

    logging.info("Flask application created and configured successfully.")
    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host='0.0.0.0', port=7890,use_reloader=False)
