//! 网络指纹：健康记录按网络维度分开（同一个接入点在 Wi-Fi 下好、在某运营商蜂窝下差是常态）。
//!
//! 由平台给出的 interface 类型 + 粗粒度提示（原生：Wi-Fi 网关 IP 前缀 / 蜂窝 MCC-MNC；
//! Web：`navigator.connection.type`）哈希而成。只在本地使用，不上报，不使用 SSID（需要定位权限）。

use std::fmt;

use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NetInterface {
    Wifi,
    Cellular,
    Ethernet,
    Other,
}

impl NetInterface {
    /// 宽松解析平台给出的接口类型字符串。
    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "wifi" | "wi-fi" | "wlan" => NetInterface::Wifi,
            "cellular" | "mobile" | "wwan" | "2g" | "3g" | "4g" | "5g" | "lte" => {
                NetInterface::Cellular
            }
            "ethernet" | "wired" | "lan" => NetInterface::Ethernet,
            _ => NetInterface::Other,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            NetInterface::Wifi => "wifi",
            NetInterface::Cellular => "cellular",
            NetInterface::Ethernet => "ethernet",
            NetInterface::Other => "other",
        }
    }
}

/// 网络指纹（64 位哈希）。`UNKNOWN` 表示平台尚未报告网络信息，所有记录共用一个桶。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct NetPrint(u64);

impl NetPrint {
    pub const UNKNOWN: NetPrint = NetPrint(0);

    /// 由接口类型与粗粒度提示得到指纹。提示做 trim + 小写归一化，避免平台大小写差异分裂记录。
    pub fn from_parts(interface: Option<&str>, hint: Option<&str>) -> Self {
        let interface = interface.map(NetInterface::parse);
        let hint = hint
            .map(|hint| hint.trim().to_ascii_lowercase())
            .filter(|hint| !hint.is_empty());
        if interface.is_none() && hint.is_none() {
            return NetPrint::UNKNOWN;
        }
        let mut hasher = Sha256::new();
        hasher.update(b"flare.netprint.v1\0");
        hasher.update(interface.map_or("", NetInterface::as_str).as_bytes());
        hasher.update([0u8]);
        hasher.update(hint.as_deref().unwrap_or("").as_bytes());
        let digest = hasher.finalize();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&digest[..8]);
        // 0 留给 UNKNOWN。
        NetPrint(u64::from_be_bytes(bytes).max(1))
    }

    pub fn from_hex(value: &str) -> Option<Self> {
        if value.len() != 16 {
            return None;
        }
        u64::from_str_radix(value, 16).ok().map(NetPrint)
    }

    pub fn to_hex(self) -> String {
        format!("{:016x}", self.0)
    }

    pub fn raw(self) -> u64 {
        self.0
    }
}

impl fmt::Display for NetPrint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalises_and_separates_networks() {
        let a = NetPrint::from_parts(Some("WiFi"), Some(" 192.168.1 "));
        let b = NetPrint::from_parts(Some("wifi"), Some("192.168.1"));
        let c = NetPrint::from_parts(Some("cellular"), Some("460-01"));
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert_ne!(a, NetPrint::UNKNOWN);
        assert_eq!(NetPrint::from_parts(None, Some("  ")), NetPrint::UNKNOWN);
        assert_eq!(NetPrint::from_hex(&a.to_hex()), Some(a));
        assert_eq!(NetInterface::parse("LTE"), NetInterface::Cellular);
    }
}
