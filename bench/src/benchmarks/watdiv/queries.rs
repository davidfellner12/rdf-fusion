use std::fmt::{Display, Formatter};

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum WatDivQueryName {
    S1, S2, S3, S4, S5, S6, S7,
    C1, C2, C3,
    F1, F2, F3, F4, F5,
    L1, L2, L3, L4, L5,
}

impl WatDivQueryName {
    pub fn list_queries() -> Vec<Self> {
        vec![
            Self::S1, Self::S2, Self::S3, Self::S4, Self::S5, Self::S6, Self::S7,
            Self::C1, Self::C2, Self::C3,
            Self::F1, Self::F2, Self::F3, Self::F4, Self::F5,
            Self::L1, Self::L2, Self::L3, Self::L4, Self::L5,
        ]
    }

    pub fn file_name(&self) -> &'static str {
        match self {
            Self::S1 => "S1.sparql", Self::S2 => "S2.sparql", Self::S3 => "S3.sparql",
            Self::S4 => "S4.sparql", Self::S5 => "S5.sparql", Self::S6 => "S6.sparql",
            Self::S7 => "S7.sparql", Self::C1 => "C1.sparql", Self::C2 => "C2.sparql",
            Self::C3 => "C3.sparql", Self::F1 => "F1.sparql", Self::F2 => "F2.sparql",
            Self::F3 => "F3.sparql", Self::F4 => "F4.sparql", Self::F5 => "F5.sparql",
            Self::L1 => "L1.sparql", Self::L2 => "L2.sparql", Self::L3 => "L3.sparql",
            Self::L4 => "L4.sparql", Self::L5 => "L5.sparql",
        }
    }
}

impl Display for WatDivQueryName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.file_name().trim_end_matches(".sparql"))
    }
}