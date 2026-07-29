from pathlib import Path

BOOKWORM = 'golang:1.25.12-bookworm@sha256:ea341baa9bd5ba6784f6d7161ace70544349a6242d54d34a0fbfd2c4d51c9d58'
DISTROLESS = 'gcr.io/distroless/static-debian12:nonroot@sha256:f5b485ea962d9bd1186b2f6b3a061191539b905b82ec395de78cbfae51f20e35'

Path('Dockerfile.space-compute-domain-agent').write_text(f'''FROM {BOOKWORM} AS build
WORKDIR /src
COPY . .
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN CGO_ENABLED=0 GOOS=${{TARGETOS}} GOARCH=${{TARGETARCH}} \\
    go build -buildvcs=false -trimpath -ldflags='-s -w' \\
    -o /out/space-compute-domain-agent ./cmd/space-compute-domain-agent

FROM {DISTROLESS}
COPY --from=build /out/space-compute-domain-agent /usr/local/bin/space-compute-domain-agent
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/space-compute-domain-agent"]
''')

p = Path('.github/workflows/space-compute-supply-chain.yml')
text = p.read_text()
needle = '          - {component: mission-planner, dockerfile: Dockerfile.space-compute-mission-planner}\n'
if text.count(needle) != 1:
    raise SystemExit('supply-chain mission-planner matrix marker missing')
text = text.replace(needle, needle + '          - {component: domain-agent, dockerfile: Dockerfile.space-compute-domain-agent}\n', 1)
p.write_text(text)

p = Path('docs/space-compute/PHASE11_SECURITY_AND_SUPPLY_CHAIN.md')
text = p.read_text().replace('builds the six\nrepository Space Compute images', 'builds all seven\nrepository Space Compute images')
p.write_text(text)
