package computors

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/qubic/go-archiver-v2/protobuf"
)

func TestComputorsModel_QubicToProtoAndBackToQubic(t *testing.T) {
	computors := Computors{
		Epoch:      1,
		TickNumber: 2,
		PubKeys: [NumberOfComputors][32]byte{
			{230, 252, 58, 173, 75, 89, 77, 130, 191, 49, 3, 161, 16, 22, 216, 13, 232, 131, 222, 135, 59, 206, 196, 142, 144, 57, 98, 134, 80, 59, 38, 19},
			{202, 170, 77, 59, 174, 172, 46, 236, 91, 33, 251, 190, 210, 221, 128, 54, 108, 203, 61, 60, 6, 180, 238, 166, 114, 128, 99, 30, 106, 188, 66, 81},
		},
		Signature: [64]byte{0x1, 0x2, 0x3, 0x4, 0xff, 0x6, 0x07, 0x8, 0x9, 0x10, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x07, 0x8, 0x9, 0x10}, //01020304ff0607080910010203040506070809100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
	}

	comps := &protobuf.Computors{
		Epoch:        1,
		TickNumber:   2,
		Identities:   []string{"QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB", "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB"},
		SignatureHex: "01020304ff0607080910010203040506070809100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
	}

	expected := &protobuf.ComputorsList{
		Computors: []*protobuf.Computors{comps},
	}

	got, err := qubicToProto([]*Computors{&computors})
	if err != nil {
		t.Fatalf("qubicToProto() error: %v", err)
	}

	if diff := cmp.Diff(&got, &expected, cmpopts.IgnoreUnexported(protobuf.ComputorsList{}, protobuf.Computors{})); diff != "" {
		t.Fatalf("qubicToProto() mismatch (-got +want):\n%s", diff)
	}

	converted, err := protoToQubic(got)
	if err != nil {
		t.Fatalf("protoToQubic() error: %v", err)
	}

	if diff := cmp.Diff(converted, []*Computors{&computors}); diff != "" {
		t.Fatalf("protoToQubic() mismatch (-got +want):\n%s", diff)
	}
}

func TestComputorsModel_qubicToProto(t *testing.T) {
	// expected
	protoComputors := &protobuf.Computors{
		Epoch:        1,
		TickNumber:   2,
		Identities:   []string{"QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB", "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB"},
		SignatureHex: "01020304ff0607080910010203040506070809100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
	}
	protoComputorsList := &protobuf.ComputorsList{
		Computors: []*protobuf.Computors{protoComputors},
	}

	// input
	qubicComputors := Computors{
		Epoch:      1,
		TickNumber: 2,
		PubKeys: [NumberOfComputors][32]byte{
			{230, 252, 58, 173, 75, 89, 77, 130, 191, 49, 3, 161, 16, 22, 216, 13, 232, 131, 222, 135, 59, 206, 196, 142, 144, 57, 98, 134, 80, 59, 38, 19},
			{202, 170, 77, 59, 174, 172, 46, 236, 91, 33, 251, 190, 210, 221, 128, 54, 108, 203, 61, 60, 6, 180, 238, 166, 114, 128, 99, 30, 106, 188, 66, 81},
		},
		Signature: [64]byte{0x1, 0x2, 0x3, 0x4, 0xff, 0x6, 0x07, 0x8, 0x9, 0x10, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x07, 0x8, 0x9, 0x10}, //01020304ff0607080910010203040506070809100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
	}
	qubicComputorsList := []*Computors{&qubicComputors}

	converted, err := qubicToProto(qubicComputorsList)
	if err != nil {
		t.Fatalf("qubicToProto() error: %v", err)
	}

	if diff := cmp.Diff(&converted, &protoComputorsList, cmpopts.IgnoreUnexported(protobuf.ComputorsList{}, protobuf.Computors{})); diff != "" {
		t.Fatalf("qubicToProto() mismatch (-got +want):\n%s", diff)
	}
}

func TestComputorsModel_protoToQubic(t *testing.T) {
	// expected
	qubicComputors := Computors{
		Epoch:      1,
		TickNumber: 2,
		PubKeys: [NumberOfComputors][32]byte{
			{230, 252, 58, 173, 75, 89, 77, 130, 191, 49, 3, 161, 16, 22, 216, 13, 232, 131, 222, 135, 59, 206, 196, 142, 144, 57, 98, 134, 80, 59, 38, 19},
			{202, 170, 77, 59, 174, 172, 46, 236, 91, 33, 251, 190, 210, 221, 128, 54, 108, 203, 61, 60, 6, 180, 238, 166, 114, 128, 99, 30, 106, 188, 66, 81},
		},
		Signature: [64]byte{0x1, 0x2, 0x3, 0x4, 0xff, 0x6, 0x07, 0x8, 0x9, 0x10, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x07, 0x8, 0x9, 0x10}, //01020304ff0607080910010203040506070809100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000
	}
	qubicComputorsList := []*Computors{&qubicComputors}

	// input
	protoComputors := &protobuf.Computors{
		Epoch:        1,
		TickNumber:   2,
		Identities:   []string{"QJRRSSKMJRDKUDTYVNYGAMQPULKAMILQQYOWBEXUDEUWQUMNGDHQYLOAJMEB", "IXTSDANOXIVIWGNDCNZVWSAVAEPBGLGSQTLSVHHBWEGKSEKPRQGWIJJCTUZB"},
		SignatureHex: "01020304ff0607080910010203040506070809100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000",
	}
	protoComputorsList := &protobuf.ComputorsList{
		Computors: []*protobuf.Computors{protoComputors},
	}

	converted, err := protoToQubic(protoComputorsList)
	if err != nil {
		t.Fatalf("protoToQubic() error: %v", err)
	}

	if diff := cmp.Diff(converted, qubicComputorsList); diff != "" {
		t.Fatalf("protoToQubic() mismatch (-got +want):\n%s", diff)
	}
}
