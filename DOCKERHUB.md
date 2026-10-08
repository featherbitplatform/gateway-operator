# featherbit operator

Kubernetes operator for the [featherbit](https://hub.docker.com/r/featherbit/featherbit) API gateway: it manages gateway routes and policies from custom resources and validates them with the gateway's own validation in an admission webhook. Single static binary, `FROM scratch`, runs as non-root.

Install with Helm: `helm install featherbit-operator oci://registry-1.docker.io/featherbit/featherbit-operator`
