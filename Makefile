all : geyser.pb.cxx geyser.grpc.pb.cxx solana-storage.pb.cxx solana-storage.grpc.pb.cxx

# Location of grpc installation (https://github.com/grpc/grpc)
PKG = /data/asiegel/pkg

PROTOC = $(PKG)/bin/protoc

GRPC_CPP_PLUGIN_PATH = $(PKG)/bin/grpc_cpp_plugin

PROTOS_PATH=-I/data/asiegel/pkg -I.

.PRECIOUS: %.grpc.pb.cxx
%.grpc.pb.cxx: %.proto
	$(PROTOC) $(PROTOS_PATH) --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) --experimental_allow_proto3_optional $<
	mv $(subst .pb.cxx,.pb.cc,$@) $@
	sed -i '1i\#include "local-pragmas.h"' $@

.PRECIOUS: %.pb.cxx
%.pb.cxx: %.proto
	$(PROTOC) $(PROTOS_PATH) --cpp_out=. --experimental_allow_proto3_optional $<
	mv $(subst .pb.cxx,.pb.cc,$@) $@
	sed -i '1i\#include "local-pragmas.h"' $@
