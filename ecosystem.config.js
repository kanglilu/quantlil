module.exports = {
  apps: [
    {
      name: "quant-pipeline",
      script: "main.py",
      interpreter: "python3",
      autorestart: true,
      max_restarts: 10,
      restart_delay: 5000,
      out_file: "logs/pm2-out.log",
      error_file: "logs/pm2-error.log",
      env: {
        PYTHONUNBUFFERED: "1"
      }
    }
  ]
};

