# Location of grpc installation (https://github.com/grpc/grpc)
PKG = /data/asiegel/pkg

CPPFLAGS += -I$(PKG)/include

GRPC_LIBS += -Wl,--start-group $(wildcard $(PKG)/lib/libgrpc*.a) -Wl,--end-group
GRPC_LIBS += -Wl,--start-group $(wildcard $(PKG)/lib/libupb_*.a) -Wl,--end-group
GRPC_LIBS += -Wl,--start-group $(wildcard $(PKG)/lib64/libprotobuf*.a) -Wl,--end-group
GRPC_LIBS += -Wl,--start-group $(wildcard $(PKG)/lib64/libabsl_*.a) -Wl,--end-group
GRPC_LIBS += $(PKG)/lib/libgpr.a
GRPC_LIBS += $(PKG)/lib/libaddress_sorting.a
GRPC_LIBS += $(PKG)/lib/libcares.a
GRPC_LIBS += $(PKG)/lib/libre2.a
GRPC_LIBS += $(PKG)/lib/libssl.a
GRPC_LIBS += $(PKG)/lib/libcrypto.a
GRPC_LIBS += $(PKG)/lib/libz.a
GRPC_LIBS += $(PKG)/lib/libutf8_range_lib.a
GRPC_LIBS += $(PKG)/lib64/libutf8_validity.a
GRPC_LIBS += -pthread -ldl -lsystemd

$(call make-bin,fd_grpc_geyser,geyser_server geys_methods geys_filter geys_fd_loop geyser.grpc.pb geyser.pb solana-storage.pb,fd_discof fd_disco fd_flamenco fd_reedsol fd_funk fd_tango fd_choreo fd_waltz fd_ballet fd_util,$(SECP256K1_LIBS) $(GRPC_LIBS))

$(call make-unit-test,test_geyser_client,test_geyser_client geyser.grpc.pb geyser.pb solana-storage.pb,fd_discof fd_disco fd_flamenco fd_reedsol fd_funk fd_tango fd_choreo fd_waltz fd_ballet fd_util,$(SECP256K1_LIBS) $(GRPC_LIBS))
