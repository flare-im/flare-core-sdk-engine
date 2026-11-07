package flare_core_harmony_cangjie_sdk.bridge

import flare_core_harmony_cangjie_sdk.contract.*

// GENERATED. Do not edit by hand.
// Thin Cangjie bridge over the Flare Core C ABI. Not wired: nothing here is linked to
// libflare_im_core_sdk_ffi yet, so every call fails with `bridge.not_implemented`.
public class FfiNativeBridge <: NativeBridge {
    public func invoke(descriptor!: NativeCallDescriptor, requestJson!: String): String {
        FfiContractVersionGuard.unavailable(descriptor.operation)
        throw FlareSdkException(
            "bridge.not_implemented",
            "FfiNativeBridge is not linked to libflare_im_core_sdk_ffi yet (cApi=${descriptor.cApi}, transport=${descriptor.transport}, returnMode=${descriptor.returnMode}, requestEncoding=${descriptor.requestEncoding}, responseEncoding=${descriptor.responseEncoding}, callback=${descriptor.callback}, handlePolicy=${descriptor.handlePolicy}).",
            operation: descriptor.operation
        )
    }

    // Not wired: host-managed connect token refresh needs the C ABI pair
    // `flare_sdk_set_connect_token_refresher` / `flare_sdk_complete_connect_token_refresh`, which this
    // bridge does not link. Once it does, the refresher runs off the SDK thread that asks and its
    // answer (None for no token) goes back exactly once per request.
    public func setConnectTokenRefresher(refresher: ?ConnectTokenRefresher): Unit {
        throw FlareSdkException(
            SdkErrorCodes.capabilityUnavailable,
            "FfiNativeBridge is not linked to libflare_im_core_sdk_ffi yet; host-managed connect token refresh is unavailable.",
            operation: SdkOperations.sdkSetConnectTokenRefresher
        )
    }
}
