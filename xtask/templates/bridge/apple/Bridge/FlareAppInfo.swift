// `FlareAppInfo` and the platform lifecycle live in the FFI-free FlarePlatformLifecycle module so
// other SDKs (social) reuse them without linking a second engine. Re-exported here: importing
// FlareCoreAppleSDK keeps them in scope.
@_exported import FlarePlatformLifecycle
