//! 链路层的 URL 解析，原生与 wasm 共用。
//!
//! 清单校验、恢复链接、镜像地址和 DoH 的判定都走这里。不用 `url` crate 是因为它带进 `idna` 和 ICU 表，
//! 这些调用点又用不到 Unicode 主机名，放进 wasm 成品不划算。
//!
//! 只解析带 `//` 的绝对 URL：scheme、主机（域名、IPv4、IPv6 字面量）、端口、路径、查询、片段。
//!
//! 与 `url`（WHATWG）的差异。两边都接受的输入，各字段相同。`url` 接受而这里拒绝的，是这些有意收紧的写法：
//!
//! - 控制字符、空白、反斜杠：`url` 会删掉或改写，同一串字符在不同解析器里可能指向不同主机。
//! - 没有 `//` 的写法（`https:host`、`mailto:x`）和 `file`、`ftp`：这批调用点都不用。
//! - userinfo：`https://good.example@evil.example/` 这类地址没有正当用途。
//! - 主机里 `[A-Za-z0-9._-]` 以外的字符，包括非 ASCII 和百分号编码；空主机、空标签。
//! - 非规范的 IPv4（`127.1`、`0x7f.0.0.1`、`017700000001`、结尾带点）。
//! - 空端口（`https://host:/`）。
//! - 路径里的 `.`、`..` 及其百分号编码形式：`url` 会折叠，这里不折叠，直接拒绝。
//!
//! 路径、查询、片段里的其它字符不拒绝，按 `url` 的编码集做百分号编码，所以预签名直链这类地址不受影响。
//!
//! 这里接受而 `url` 拒绝的只有一类：含 `xn--` 标签、Punycode 无效的主机。`xn--` 标签是纯 ASCII，这里按
//! 普通标签处理，不解码：拒绝它们没有安全收益，还会挡住合法的国际化域名；放行的无效标签解析不出地址，
//! 也配不上证书，走不到连接成功。
//!
//! `tests/vectors/url_parse.json` 和本文件的差分测试守着这些边界。

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// URL 不符合本模块接受的写法。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum UrlError {
    #[error("URL contains a control character, whitespace or a backslash")]
    ForbiddenCharacter,
    #[error("URL has no valid scheme")]
    InvalidScheme,
    #[error("URL scheme is not supported")]
    UnsupportedScheme,
    #[error("URL has no // authority")]
    MissingAuthority,
    #[error("URL must not carry user information")]
    UserInfo,
    #[error("URL host is invalid")]
    InvalidHost,
    #[error("URL port is invalid")]
    InvalidPort,
    #[error("URL path must not contain dot segments")]
    DotSegment,
}

/// 解析并规范化后的 URL。主机一定存在，所以 [`ParsedUrl::host_str`] 不是 `Option`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedUrl {
    scheme: String,
    host: String,
    ip: Option<IpAddr>,
    port: Option<u16>,
    path: String,
    query: Option<String>,
    fragment: Option<String>,
}

impl ParsedUrl {
    pub fn parse(input: &str) -> Result<Self, UrlError> {
        let forbidden = |byte: u8| byte <= b' ' || byte == 0x7f || byte == b'\\';
        if input.bytes().any(forbidden) {
            return Err(UrlError::ForbiddenCharacter);
        }
        let (scheme, rest) = input.split_once(':').ok_or(UrlError::InvalidScheme)?;
        let scheme = parse_scheme(scheme)?;
        let special = default_port(&scheme).is_some();
        let rest = rest.strip_prefix("//").ok_or(UrlError::MissingAuthority)?;

        let (rest, fragment) = split_tail(rest, '#');
        let (rest, query) = split_tail(rest, '?');
        let (authority, path) = rest.split_at(rest.find('/').unwrap_or(rest.len()));

        let (host, ip, port) = parse_authority(authority, special)?;
        let query_set = if special {
            in_special_query_set
        } else {
            in_query_set
        };
        Ok(Self {
            port: port.filter(|port| default_port(&scheme) != Some(*port)),
            path: parse_path(path, special)?,
            query: query.map(|query| percent_encode(query, query_set)),
            fragment: fragment.map(|fragment| percent_encode(fragment, in_fragment_set)),
            scheme,
            host,
            ip,
        })
    }

    /// 小写的 scheme。
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// 序列化形式的主机：special scheme 的域名是小写，其余保持原样，IPv6 带方括号。
    pub fn host_str(&self) -> &str {
        &self.host
    }

    /// 主机是 IP 字面量时的地址。与 `url` 一致，非 special scheme 的点分十进制只算域名。
    pub fn ip(&self) -> Option<IpAddr> {
        self.ip
    }

    /// 显式端口。等于该 scheme 的默认端口时为 `None`。
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn query(&self) -> Option<&str> {
        self.query.as_deref()
    }

    pub fn fragment(&self) -> Option<&str> {
        self.fragment.as_deref()
    }

    /// 按 `application/x-www-form-urlencoded` 解码查询串，顺序与原串相同。
    pub fn query_pairs(&self) -> impl Iterator<Item = (String, String)> + '_ {
        self.query
            .as_deref()
            .unwrap_or_default()
            .split('&')
            .filter(|pair| !pair.is_empty())
            .map(|pair| {
                let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
                (form_decode(name), form_decode(value))
            })
    }

    /// 在查询串末尾追加一对参数，按 `application/x-www-form-urlencoded` 编码。
    pub fn append_query_pair(&mut self, name: &str, value: &str) {
        let query = self.query.get_or_insert_with(String::new);
        if !query.is_empty() {
            query.push('&');
        }
        form_encode_into(query, name);
        query.push('=');
        form_encode_into(query, value);
    }
}

impl fmt::Display for ParsedUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://{}", self.scheme, self.host)?;
        if let Some(port) = self.port {
            write!(f, ":{port}")?;
        }
        f.write_str(&self.path)?;
        if let Some(query) = &self.query {
            write!(f, "?{query}")?;
        }
        if let Some(fragment) = &self.fragment {
            write!(f, "#{fragment}")?;
        }
        Ok(())
    }
}

fn split_tail(text: &str, separator: char) -> (&str, Option<&str>) {
    text.split_once(separator)
        .map_or((text, None), |(head, tail)| (head, Some(tail)))
}

fn parse_scheme(raw: &str) -> Result<String, UrlError> {
    let valid = raw.as_bytes().first().is_some_and(u8::is_ascii_alphabetic)
        && raw
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'-' | b'.'));
    if !valid {
        return Err(UrlError::InvalidScheme);
    }
    let scheme = raw.to_ascii_lowercase();
    if matches!(scheme.as_str(), "file" | "ftp") {
        return Err(UrlError::UnsupportedScheme);
    }
    Ok(scheme)
}

/// special scheme 里这批调用点用到的四个。`ftp` 和 `file` 已在 [`parse_scheme`] 拒绝。
fn default_port(scheme: &str) -> Option<u16> {
    match scheme {
        "http" | "ws" => Some(80),
        "https" | "wss" => Some(443),
        _ => None,
    }
}

type Authority = (String, Option<IpAddr>, Option<u16>);

fn parse_authority(authority: &str, special: bool) -> Result<Authority, UrlError> {
    if authority.contains('@') {
        return Err(UrlError::UserInfo);
    }
    if let Some(rest) = authority.strip_prefix('[') {
        let (literal, tail) = rest.split_once(']').ok_or(UrlError::InvalidHost)?;
        let address: Ipv6Addr = literal.parse().map_err(|_| UrlError::InvalidHost)?;
        let port = if tail.is_empty() {
            None
        } else {
            let port = tail.strip_prefix(':').ok_or(UrlError::InvalidHost)?;
            Some(parse_port(port)?)
        };
        return Ok((ipv6_text(address), Some(IpAddr::V6(address)), port));
    }
    let (host, port) = split_tail(authority, ':');
    let (host, ip) = parse_domain(host, special)?;
    Ok((host, ip, port.map(parse_port).transpose()?))
}

fn parse_port(raw: &str) -> Result<u16, UrlError> {
    // `u16::from_str` 接受开头的 `+`，所以先查字符。
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(UrlError::InvalidPort);
    }
    raw.parse().map_err(|_| UrlError::InvalidPort)
}

/// 与 `url` 同样的序列化：IPv4 映射地址也写成十六进制，标准库的 `Display` 写成点分十进制。
fn ipv6_text(address: Ipv6Addr) -> String {
    if address.to_ipv4_mapped().is_some() {
        let segments = address.segments();
        return format!("[::ffff:{:x}:{:x}]", segments[6], segments[7]);
    }
    format!("[{address}]")
}

fn parse_domain(raw: &str, special: bool) -> Result<(String, Option<IpAddr>), UrlError> {
    let host_byte = |byte: u8| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_');
    let stem = raw.strip_suffix('.').unwrap_or(raw);
    if !raw.bytes().all(host_byte) || stem.split('.').any(str::is_empty) {
        return Err(UrlError::InvalidHost);
    }
    if !special {
        return Ok((raw.to_owned(), None));
    }
    let host = raw.to_ascii_lowercase();
    let stem = host.strip_suffix('.').unwrap_or(&host);
    // WHATWG 把最后一级是数字的主机一律当 IPv4，这里只接受规范的四段写法。
    if stem.rsplit('.').next().is_some_and(is_ipv4_number) {
        let address: Ipv4Addr = host.parse().map_err(|_| UrlError::InvalidHost)?;
        return Ok((host, Some(IpAddr::V4(address))));
    }
    Ok((host, None))
}

/// 已转成小写的标签是不是 WHATWG 的 IPv4 数字：十进制、八进制，或 `0x` 开头的十六进制（可以没有位数）。
fn is_ipv4_number(label: &str) -> bool {
    match label.strip_prefix("0x") {
        Some(digits) => digits.bytes().all(|byte| byte.is_ascii_hexdigit()),
        None => label.bytes().all(|byte| byte.is_ascii_digit()),
    }
}

fn parse_path(raw: &str, special: bool) -> Result<String, UrlError> {
    if raw.split('/').any(is_dot_segment) {
        return Err(UrlError::DotSegment);
    }
    if special && raw.is_empty() {
        return Ok("/".to_owned());
    }
    Ok(percent_encode(raw, in_path_set))
}

fn is_dot_segment(segment: &str) -> bool {
    const DOT_SEGMENTS: [&str; 6] = [".", "..", "%2e", ".%2e", "%2e.", "%2e%2e"];
    DOT_SEGMENTS
        .iter()
        .any(|dots| segment.eq_ignore_ascii_case(dots))
}

// 控制字符和空格已在入口拒绝，下面四个编码集只需列出其余字符，取自 `url` 的 FRAGMENT、PATH、QUERY、SPECIAL_QUERY。
fn in_fragment_set(byte: u8) -> bool {
    !byte.is_ascii() || matches!(byte, b'"' | b'<' | b'>' | b'`')
}

fn in_path_set(byte: u8) -> bool {
    in_fragment_set(byte) || matches!(byte, b'#' | b'?' | b'{' | b'}')
}

fn in_query_set(byte: u8) -> bool {
    !byte.is_ascii() || matches!(byte, b'"' | b'#' | b'<' | b'>')
}

fn in_special_query_set(byte: u8) -> bool {
    in_query_set(byte) || byte == b'\''
}

fn percent_encode(raw: &str, encoded: fn(u8) -> bool) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        if encoded(byte) {
            push_percent(&mut out, byte);
        } else {
            out.push(char::from(byte));
        }
    }
    out
}

fn push_percent(out: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    out.push('%');
    out.push(char::from(HEX[usize::from(byte >> 4)]));
    out.push(char::from(HEX[usize::from(byte & 0x0f)]));
}

fn form_encode_into(out: &mut String, raw: &str) {
    for byte in raw.bytes() {
        match byte {
            b' ' => out.push('+'),
            b'*' | b'-' | b'.' | b'_' => out.push(char::from(byte)),
            _ if byte.is_ascii_alphanumeric() => out.push(char::from(byte)),
            _ => push_percent(out, byte),
        }
    }
}

fn form_decode(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let hex = |at: usize| {
        bytes
            .get(at)
            .and_then(|byte| char::from(*byte).to_digit(16))
    };
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        match (bytes[at], hex(at + 1), hex(at + 2)) {
            (b'+', ..) => out.push(b' '),
            (b'%', Some(high), Some(low)) => {
                out.push((high * 16 + low) as u8);
                at += 2;
            }
            (byte, ..) => out.push(byte),
        }
        at += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 主机里有 `xn--` 标签：`url` 校验 Punycode，这里不校验，这是两者唯一允许的宽松差异。
    fn has_ace_label(url: &ParsedUrl) -> bool {
        url.host_str()
            .split('.')
            .any(|label| label.starts_with("xn--"))
    }

    /// 这里接受且 `url` 也接受的输入，每个字段都必须相同。
    fn assert_matches_url_crate(input: &str, ours: &ParsedUrl) {
        let theirs = url::Url::parse(input)
            .unwrap_or_else(|error| panic!("{input:?} accepted here, url says {error}"));
        assert_eq!(ours.scheme(), theirs.scheme(), "{input:?}");
        assert_eq!(Some(ours.host_str()), theirs.host_str(), "{input:?}");
        assert_eq!(ours.port(), theirs.port(), "{input:?}");
        assert_eq!(ours.path(), theirs.path(), "{input:?}");
        assert_eq!(ours.query(), theirs.query(), "{input:?}");
        assert_eq!(ours.fragment(), theirs.fragment(), "{input:?}");
        assert_eq!(ours.to_string(), theirs.as_str(), "{input:?}");
        let ip = match theirs.host() {
            Some(url::Host::Ipv4(address)) => Some(IpAddr::V4(address)),
            Some(url::Host::Ipv6(address)) => Some(IpAddr::V6(address)),
            _ => None,
        };
        assert_eq!(ours.ip(), ip, "{input:?}");
        let pairs: Vec<_> = theirs.query_pairs().into_owned().collect();
        assert_eq!(ours.query_pairs().collect::<Vec<_>>(), pairs, "{input:?}");
        let mut ours = ours.clone();
        let mut theirs = theirs;
        ours.append_query_pair("k y", "a+b/c&é=");
        theirs.query_pairs_mut().append_pair("k y", "a+b/c&é=");
        assert_eq!(ours.to_string(), theirs.as_str(), "{input:?}");
    }

    /// 片段随机拼接：比逐字符随机更容易凑出语法上接近合法的 URL。
    #[rustfmt::skip]
    const PIECES: &[&str] = &[
        "http", "https", "ws", "wss", "quic", "flare-recover", "HTTPS", "ftp", "file", "foo", ":",
        "//", "/", "///", "\\", "@", "?", "#", "&", "=", "+", "%", "%2e", "%2E", ".", "..", "[",
        "]", "::1", "::ffff:1.2.3.4", "1:2:3:4:5:6:7:8", "fe80::1%25en0", "a", "A", "example.com",
        "EXAMPLE.com", "good.example", "xn--", "xn--a", "XN--fiqs8s", "xn--fiqs8s", "127.0.0.1", "127.1", "0x7f",
        "017700000001", "2130706433", "256", "1.2.3.4", "0", "80", "443", "65535", "65536", "-",
        "_", "~", "!", "$", "'", "\"", "<", ">", "`", "{", "}", "|", "^", " ", "\t", "\n", "\r",
        "\u{0}", "\u{1f}", "\u{7f}", "\u{a0}", "é", "例子", "\u{3000}", "\u{ff0e}", "%00", "%0a",
        "%20", "%2f", "%5c", "%zz", "v1", "p=1", "dns=x",
    ];

    #[test]
    fn only_ace_labels_are_wider_than_the_url_crate() {
        // xorshift：固定种子，失败可复现，也不用为测试引入随机数依赖。
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = move |bound: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % bound as u64) as usize
        };
        let (mut accepted, mut ace_only) = (0, 0);
        for round in 0..400_000 {
            // 一半固定成 `scheme://`，否则绝大多数输入在 scheme 那一步就被拒绝了。
            let mut input = String::new();
            if round % 2 == 0 {
                input.push_str(PIECES[next(10)]);
                input.push_str("://");
            }
            for _ in 0..next(9) {
                input.push_str(PIECES[next(PIECES.len())]);
            }
            if let Ok(ours) = ParsedUrl::parse(&input) {
                accepted += 1;
                if has_ace_label(&ours) && url::Url::parse(&input).is_err() {
                    ace_only += 1;
                    continue;
                }
                assert_matches_url_crate(&input, &ours);
            }
        }
        assert!(
            accepted > 10_000,
            "only {accepted} inputs reached the comparison"
        );
        assert!(ace_only > 0, "no input exercised the ACE exception");
    }

    #[test]
    fn keeps_the_fields_the_call_sites_read() {
        let url =
            ParsedUrl::parse("HTTPS://CDN.Example.com:8443/a/b%20c?x=1&y=%E4%BD%A0#top").unwrap();
        assert_eq!(url.scheme(), "https");
        assert_eq!(url.host_str(), "cdn.example.com");
        assert_eq!(url.port(), Some(8443));
        assert_eq!(url.path(), "/a/b%20c");
        assert_eq!(url.query(), Some("x=1&y=%E4%BD%A0"));
        assert_eq!(url.fragment(), Some("top"));
        assert_eq!(
            url.query_pairs().collect::<Vec<_>>(),
            [
                ("x".to_owned(), "1".to_owned()),
                ("y".to_owned(), "你".to_owned())
            ]
        );
    }

    #[test]
    fn default_ports_are_dropped_only_for_special_schemes() {
        for (input, port) in [
            ("https://a.example:443/", None),
            ("wss://a.example:443/", None),
            ("http://a.example:80/", None),
            ("ws://a.example:80/", None),
            ("https://a.example:80/", Some(80)),
            ("quic://a.example:443", Some(443)),
        ] {
            assert_eq!(ParsedUrl::parse(input).unwrap().port(), port, "{input}");
        }
    }

    #[test]
    fn ip_literals_are_reported_as_addresses() {
        let v4 = ParsedUrl::parse("http://127.0.0.1:8080/").unwrap();
        assert!(v4.ip().is_some_and(|ip| ip.is_loopback()));
        let v6 = ParsedUrl::parse("http://[::1]:8080/").unwrap();
        assert_eq!(v6.host_str(), "[::1]");
        assert!(v6.ip().is_some_and(|ip| ip.is_loopback()));
        let mapped = ParsedUrl::parse("http://[::ffff:127.0.0.1]/").unwrap();
        assert_eq!(mapped.host_str(), "[::ffff:7f00:1]");
        assert!(!mapped.ip().is_some_and(|ip| ip.is_loopback()));
        // 非 special scheme 的主机不解释，和 `url` 一样只算域名。
        assert_eq!(ParsedUrl::parse("quic://1.2.3.4:9000").unwrap().ip(), None);
    }

    #[test]
    fn rejects_what_the_url_crate_would_repair() {
        for (input, error) in [
            (" https://a.example/", UrlError::ForbiddenCharacter),
            ("https://a.ex\tample/", UrlError::ForbiddenCharacter),
            (
                "https://a.example\\@b.example/",
                UrlError::ForbiddenCharacter,
            ),
            ("https:a.example/", UrlError::MissingAuthority),
            ("https:/a.example/", UrlError::MissingAuthority),
            ("https://u:p@a.example/", UrlError::UserInfo),
            ("https://@a.example/", UrlError::UserInfo),
            ("https://例子.cn/", UrlError::InvalidHost),
            ("https://ex%61mple.com/", UrlError::InvalidHost),
            ("https://a..example/", UrlError::InvalidHost),
            ("https://127.1/", UrlError::InvalidHost),
            ("https://0x7f.0.0.1/", UrlError::InvalidHost),
            ("https://1.2.3.4./", UrlError::InvalidHost),
            ("https://a.example:/", UrlError::InvalidPort),
            ("https://a.example/b/../c", UrlError::DotSegment),
            ("https://a.example/%2E%2e/c", UrlError::DotSegment),
            ("file:///etc/passwd", UrlError::UnsupportedScheme),
        ] {
            assert_eq!(ParsedUrl::parse(input), Err(error), "{input}");
        }
    }

    #[test]
    fn ace_labels_pass_as_plain_ascii() {
        // 有效的国际化域名：字段与 `url` 相同。
        let valid = "https://XN--fiqs8s.example:8443/a";
        let parsed = ParsedUrl::parse(valid).unwrap();
        assert_eq!(parsed.host_str(), "xn--fiqs8s.example");
        assert_matches_url_crate(valid, &parsed);
        // 无效的 Punycode 这里也放行，`url` 拒绝。
        let invalid = "https://xn--a.example/";
        assert_eq!(
            ParsedUrl::parse(invalid).unwrap().host_str(),
            "xn--a.example"
        );
        assert!(url::Url::parse(invalid).is_err());
        // 原始 Unicode 仍然拒绝。
        assert_eq!(
            ParsedUrl::parse("https://例子.cn/"),
            Err(UrlError::InvalidHost)
        );
    }

    #[test]
    fn query_pairs_decode_like_form_urlencoded() {
        let url = ParsedUrl::parse("flare-recover://v1?p=a+b&&k=%2B%zz&s&=v").unwrap();
        assert_eq!(
            url.query_pairs().collect::<Vec<_>>(),
            [
                ("p".to_owned(), "a b".to_owned()),
                ("k".to_owned(), "+%zz".to_owned()),
                ("s".to_owned(), String::new()),
                (String::new(), "v".to_owned()),
            ]
        );
    }
}
