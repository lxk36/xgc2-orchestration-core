package contracts

import "testing"

func TestPortableVersionAcceptsSemVerWithoutWeakeningIdentifiers(t *testing.T) {
	for _, value := range []string{"1.0.0", "1.4.0-rc.1+lab", "v2", "source-dev"} {
		if !ValidVersion(value) {
			t.Fatalf("portable version %q was rejected", value)
		}
	}
	for _, value := range []string{"", " 1.0.0", "1/2", "版本 1"} {
		if ValidVersion(value) {
			t.Fatalf("invalid portable version %q was accepted", value)
		}
	}
	if ValidIdentifier("1.0.0") {
		t.Fatal("SemVer unexpectedly became a resource identifier")
	}
}
