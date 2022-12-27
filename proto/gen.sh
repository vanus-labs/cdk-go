PROJECT_ROOT=$( pwd )/..

mkdir -p "${PROJECT_ROOT}"/proto
protoc -I="${PROJECT_ROOT}"/../vanus/proto/include \
         -I="${PROJECT_ROOT}"/proto \
         --go_out=plugins=grpc,paths=source_relative:"${PROJECT_ROOT}"/proto \
         "${PROJECT_ROOT}"/proto/cloudevents.proto