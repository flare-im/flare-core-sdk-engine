pub(super) const MAX_WEB_FILE_SIZE: u64 = 10 * 1024 * 1024 * 1024;
pub(super) const WEB_FILE_PART_SIZE: u64 = 16 * 1024 * 1024;

pub(super) struct WebFileRange {
    start: u64,
    end: u64,
    size: u64,
    len: u64,
}

impl WebFileRange {
    pub(super) fn new(start: u64, len: u64, size: u64) -> Option<Self> {
        if len == 0 || len > WEB_FILE_PART_SIZE || size > MAX_WEB_FILE_SIZE {
            return None;
        }
        let end = start.checked_add(len)? - 1;
        (end < size).then_some(Self {
            start,
            end,
            size,
            len,
        })
    }

    pub(super) fn request_header(&self) -> String {
        format!("bytes={}-{}", self.start, self.end)
    }

    pub(super) fn matches_response(
        &self,
        status: u16,
        content_range: Option<&str>,
        content_length: Option<&str>,
    ) -> bool {
        let expected_range = format!("bytes {}-{}/{}", self.start, self.end, self.size);
        status == 206
            && content_range == Some(expected_range.as_str())
            && content_length.and_then(|len| len.parse::<u64>().ok()) == Some(self.len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_a_range_beyond_wasm32_address_space() {
        let file_size = MAX_WEB_FILE_SIZE;
        let range = WebFileRange::new(file_size - 7, 7, file_size).unwrap();
        assert_eq!(range.request_header(), "bytes=10737418233-10737418239");

        let content_range = Some("bytes 10737418233-10737418239/10737418240");
        assert!(range.matches_response(206, content_range, Some("7")));
        assert!(!range.matches_response(200, content_range, Some("7")));
        assert!(!range.matches_response(206, content_range, Some("8")));
        assert!(!range.matches_response(206, content_range, None));
        assert!(!range.matches_response(
            206,
            Some("bytes 10737418233-10737418239/10737418241"),
            Some("7")
        ));
    }

    #[test]
    fn rejects_empty_overflowing_overlong_and_out_of_bounds_ranges() {
        assert!(WebFileRange::new(0, 0, 1).is_none());
        assert!(WebFileRange::new(u64::MAX, 1, MAX_WEB_FILE_SIZE).is_none());
        assert!(WebFileRange::new(0, 1, MAX_WEB_FILE_SIZE + 1).is_none());
        assert!(WebFileRange::new(0, WEB_FILE_PART_SIZE + 1, MAX_WEB_FILE_SIZE).is_none());
        assert!(WebFileRange::new(MAX_WEB_FILE_SIZE - 7, 8, MAX_WEB_FILE_SIZE).is_none());
    }
}
