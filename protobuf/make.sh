GPA=$(go env GOPATH)
export PATH="${PATH}:${GPA}/bin"

make all
