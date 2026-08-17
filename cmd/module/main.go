// Command walkie serves this module's models to viam-server.
package main

import (
	"go.viam.com/rdk/components/audioin"
	"go.viam.com/rdk/components/audioout"
	"go.viam.com/rdk/components/generic"
	toggleswitch "go.viam.com/rdk/components/switch"
	"go.viam.com/rdk/module"
	"go.viam.com/rdk/resource"

	"walkie"
)

func main() {
	// The hub models and the member models ship in one binary on purpose: a
	// single machine can be both, which is how you try the whole thing out
	// before wiring up a second one.
	module.ModularMain(
		resource.APIModel{API: generic.API, Model: walkie.Bus},
		resource.APIModel{API: audioout.API, Model: walkie.Uplink},
		resource.APIModel{API: audioin.API, Model: walkie.Downlink},
		resource.APIModel{API: generic.API, Model: walkie.Radio},
		resource.APIModel{API: toggleswitch.API, Model: walkie.Switch},
	)
}
