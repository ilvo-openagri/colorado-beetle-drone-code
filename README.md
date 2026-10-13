# OpenAgri Drone

OpenAgri Drone is a Linux application for capturing images from a connected
camera and sending image data and capture metadata to the configured OpenAgri
services. It supports a GPhoto2-compatible camera or an image-directory test
camera, and provides an HTTP API for controlling capture.

## Getting started

See the [installation guide](documentation/INSTALL.md) for system requirements,
Python setup, service configuration, and run instructions.

## Runtime API

The HTTP server exposes:

- `POST /start` to start capturing. The JSON body must include `project_id`,
  `flight_id`, and `ads` (`edge` or `cloud`).
- `POST /stop` to stop capture.
- `GET /status` to read capture status.
- `GET /video_feed` to view the camera stream.

## License and funding

This project is licensed under the Apache License, Version 2.0; see
[LICENSE](LICENSE). Project attribution and funding information are in
[NOTICE](NOTICE).
