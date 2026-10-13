# Installation

This guide covers a Debian or Ubuntu Linux installation. The application needs
Python 3.9 or later, a GPhoto2-compatible camera for live capture, and access to
the PostgreSQL, MinIO, and Telegraf services configured for the deployment.

## 1. Install system packages

Install Python, virtual-environment support, and the camera/database libraries:

```sh
sudo apt update
sudo apt install python3 python3-venv python3-pip \
  libexif12 libgphoto2-6 libgphoto2-port12 libltdl7 libpq-dev gphoto2
```

Package names can differ between Linux distributions. Install the equivalent
GPhoto2 and PostgreSQL development libraries if using a different distribution.

## 2. Set up the Python environment

Clone the repository and install its Python dependencies:

```sh
git clone https://gitlab.ilvo.be/openagri/drone.git
cd drone
python3 -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements.txt
```

If the repository has already been checked out, run these commands from its
root directory instead of cloning it again.

## 3. Configure the deployment

The application reads `.env.edge` and `.env.cloud` from its working directory by
default. Start with the checked-in templates, then replace their example values
with settings supplied by the deployment administrator:

```sh
cp .env.example .env.edge
cp .env.example .env.cloud
```

Each file must provide the service settings used by its connector:

```text
TELEGRAF_IP
TELEGRAF_PORT
MINIO_URL
MINIO_ACCESS_KEY
MINIO_SECRET_KEY
MINIO_RAW_BUCKET
PG_CLOUD_HOST
PG_CLOUD_PORT
PG_CLOUD_DATABASE
PG_CLOUD_USER
PG_CLOUD_PASSWORD
QUERY_FILE
```

`DJI_GPS_URL` is optional. If supplied, `DJI_GPS_INTERVAL_S` and
`DJI_GPS_TIMEOUT_S` can optionally configure sampling (defaults: `0.25` and
`2.0` seconds). Do not commit credentials or deployment-specific configuration
to the repository.

For the ILVO network deployment, add these host mappings to `/etc/hosts` if
directed by the deployment administrator:

```text
192.168.50.1    ilvonuc.com
192.168.50.1    postgres.ilvonuc.com
127.0.1.1       api.minio.ilvoraspi.com
```

These mappings are deployment-specific; use the addresses supplied for your
environment.

## 4. Start the application

From the repository root, start the live-camera runtime and bind its HTTP API to
the desired interface and port:

```sh
PYTHONPATH=src .venv/bin/python -m main_new --app-ip 127.0.0.1 --app-port 5000
```

Use an interface reachable by your clients instead of `127.0.0.1` when remote
clients need to access the API. The runtime defaults to `192.168.50.84:5000`.
Run `PYTHONPATH=src .venv/bin/python -m main_new --help` to see all supported
options, including alternate environment-file paths and logging levels.

To use an image directory instead of a physical camera:

```sh
PYTHONPATH=src .venv/bin/python -m main_new \
  --test --images /path/to/images \
  --app-ip 127.0.0.1 --app-port 5000
```

Test mode replaces camera capture with images from the specified directory; it
does not disable the database, MinIO, or Telegraf connectors. Both environment
files and access to the configured services are still required.

## 5. Control capture

For example, with the API bound to localhost:

```sh
curl -X POST http://127.0.0.1:5000/start \
  -H 'Content-Type: application/json' \
  -d '{"project_id": 1, "flight_id": 1, "ads": "edge"}'
curl http://127.0.0.1:5000/status
curl -X POST http://127.0.0.1:5000/stop
```

Use `"ads": "cloud"` to select the cloud connector.

## Optional: run with systemd

The repository includes `openagri_drone.service` as a deployment starting
point. Before installing it, update its working directory, Python executable,
service user, and log destination for the target machine. Ensure that the
service user can access the camera and configuration files, and can write to
the log directory. Then install and enable the unit:

```sh
sudo cp openagri_drone.service /etc/systemd/system/openagri_drone.service
sudo systemctl daemon-reload
sudo systemctl enable --now openagri_drone.service
```
