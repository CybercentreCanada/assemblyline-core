

build: Dockerfile assemblyline-kubernetes
	docker build . -t localhost:32000/cccs/assemblyline-metrics-adaptor:stable

push: build
	docker push localhost:32000/cccs/assemblyline-metrics-adaptor:stable

assemblyline-kubernetes: main.go internal/fetch.go internal/metrics.go
	CGO_ENABLED=0 go build