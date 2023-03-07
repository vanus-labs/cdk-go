PROJECT_ROOT=$(pwd)/..

mkdir -p "${PROJECT_ROOT}"/proto
protoc -I="${PROJECT_ROOT}"/../vanus/proto/include \
        -I="${PROJECT_ROOT}"/proto \
        --go_out="${PROJECT_ROOT}"/proto \
        --go_opt=paths=source_relative \
        --go-grpc_out="${PROJECT_ROOT}"/proto \
        --go-grpc_opt=paths=source_relative,require_unimplemented_servers=false \
        "${PROJECT_ROOT}"/proto/cloudevents.proto
