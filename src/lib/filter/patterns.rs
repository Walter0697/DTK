use crate::FilterConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum PathSegment {
    Key(String),
    AnyIndex,
    Index(usize),
    AnyDescendant,
}

#[derive(Debug, Clone)]
pub(super) struct PathPattern {
    pub(super) segments: Vec<PathSegment>,
}

impl PathPattern {
    pub(super) fn parse(pattern: &str) -> Self {
        let mut segments = Vec::new();

        for part in pattern.split('.') {
            if part.is_empty() {
                continue;
            }

            if part == "**" {
                segments.push(PathSegment::AnyDescendant);
                break;
            }

            let mut remainder = part;
            loop {
                if let Some(open) = remainder.find('[') {
                    let (prefix, rest) = remainder.split_at(open);
                    if !prefix.is_empty() {
                        segments.push(PathSegment::Key(prefix.to_string()));
                    }

                    if let Some(close) = rest.find(']') {
                        let index_text = &rest[1..close];
                        if index_text.is_empty() {
                            segments.push(PathSegment::AnyIndex);
                        } else if let Ok(index) = index_text.parse::<usize>() {
                            segments.push(PathSegment::Index(index));
                        } else {
                            segments.push(PathSegment::Key(rest[..=close].to_string()));
                        }
                        remainder = &rest[close + 1..];
                        if remainder.is_empty() {
                            break;
                        }
                    } else {
                        if !remainder.is_empty() {
                            segments.push(PathSegment::Key(remainder.to_string()));
                        }
                        break;
                    }
                } else {
                    segments.push(PathSegment::Key(remainder.to_string()));
                    break;
                }
            }
        }

        Self { segments }
    }

    pub(super) fn strip_first_any_index(mut self) -> Self {
        if matches!(self.segments.first(), Some(PathSegment::AnyIndex)) {
            self.segments.remove(0);
        }
        self
    }

    pub(super) fn covers_path(&self, path: &[PathSegment]) -> bool {
        match self.segments.last() {
            Some(PathSegment::AnyDescendant) => {
                let fixed_len = self.segments.len().saturating_sub(1);
                path.len() >= fixed_len
                    && self.segments[..fixed_len]
                        .iter()
                        .zip(path.iter())
                        .all(|(pattern, actual)| segment_matches(pattern, actual))
            }
            _ => {
                path.len() >= self.segments.len()
                    && self
                        .segments
                        .iter()
                        .zip(path.iter())
                        .all(|(pattern, actual)| segment_matches(pattern, actual))
            }
        }
    }

    pub(super) fn matches_exact(&self, path: &[PathSegment]) -> bool {
        match self.segments.last() {
            Some(PathSegment::AnyDescendant) => {
                let fixed_len = self.segments.len().saturating_sub(1);
                path.len() >= fixed_len
                    && self.segments[..fixed_len]
                        .iter()
                        .zip(path.iter())
                        .all(|(pattern, actual)| segment_matches(pattern, actual))
            }
            _ => {
                self.segments.len() == path.len()
                    && self
                        .segments
                        .iter()
                        .zip(path.iter())
                        .all(|(pattern, actual)| segment_matches(pattern, actual))
            }
        }
    }

    pub(super) fn path_is_prefix(&self, path: &[PathSegment]) -> bool {
        match self.segments.last() {
            Some(PathSegment::AnyDescendant) => {
                let fixed_len = self.segments.len().saturating_sub(1);
                path.len() >= fixed_len
                    && self.segments[..fixed_len]
                        .iter()
                        .zip(path.iter())
                        .all(|(pattern, actual)| segment_matches(pattern, actual))
            }
            _ => {
                path.len() <= self.segments.len()
                    && path
                        .iter()
                        .zip(self.segments.iter())
                        .all(|(actual, pattern)| segment_matches(pattern, actual))
            }
        }
    }
}

pub fn normalize_field_path_for_config(field_path: &str, config: &FilterConfig) -> Option<String> {
    let mut normalized = field_path.trim();
    if let Some(content_path) = config
        .content_path
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if let Some(remainder) = normalized.strip_prefix(content_path) {
            if remainder.is_empty() || remainder.starts_with('.') || remainder.starts_with('[') {
                normalized = remainder;
            }
        }
    }

    let normalized = normalized.trim();
    if normalized.is_empty() {
        None
    } else {
        Some(normalized.to_string())
    }
}

pub(crate) fn normalize_repeated_field_path(field: &str) -> Option<String> {
    let mut pattern = PathPattern::parse(field.trim());
    if pattern.segments.is_empty() {
        return None;
    }

    for segment in &mut pattern.segments {
        if matches!(segment, PathSegment::Index(_)) {
            *segment = PathSegment::AnyIndex;
        }
    }

    Some(render_field_path(&pattern.segments))
}

pub(super) fn normalize_path_pattern_for_config(
    pattern: PathPattern,
    config: &FilterConfig,
) -> Option<PathPattern> {
    let rendered = render_field_path(&pattern.segments);
    let normalized = normalize_field_path_for_config(&rendered, config)?;
    let pattern = PathPattern::parse(&normalized);
    if pattern.segments.is_empty() {
        None
    } else {
        Some(pattern)
    }
}

pub(super) fn render_field_path(path: &[PathSegment]) -> String {
    let mut out = String::new();
    for segment in path {
        match segment {
            PathSegment::Key(key) => {
                if out.is_empty() {
                    out.push_str(key);
                } else {
                    out.push('.');
                    out.push_str(key);
                }
            }
            PathSegment::AnyIndex => out.push_str("[]"),
            PathSegment::Index(index) => {
                out.push('[');
                out.push_str(&index.to_string());
                out.push(']');
            }
            PathSegment::AnyDescendant => {
                if !out.is_empty() {
                    out.push('.');
                }
                out.push_str("**");
            }
        }
    }
    out
}

fn segment_matches(pattern: &PathSegment, actual: &PathSegment) -> bool {
    match (pattern, actual) {
        (PathSegment::AnyIndex, PathSegment::AnyIndex) => true,
        (PathSegment::AnyIndex, PathSegment::Index(_)) => true,
        (PathSegment::AnyDescendant, _) => true,
        (PathSegment::Index(left), PathSegment::Index(right)) => left == right,
        (PathSegment::Key(left), PathSegment::Key(right)) => left == right,
        _ => false,
    }
}
