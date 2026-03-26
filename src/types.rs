use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaxNode {
    pub name: String,
    pub description: Option<String>,
    pub chemont_id: String,
    pub url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExternalDescriptor {
    pub source: String,
    pub source_id: String,
    #[serde(default)]
    pub annotations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EntityResponse {
    pub smiles: Option<String>,
    pub inchikey: Option<String>,
    pub kingdom: Option<TaxNode>,
    pub superclass: Option<TaxNode>,
    #[serde(rename = "class")]
    pub class_node: Option<TaxNode>,
    pub subclass: Option<TaxNode>,
    #[serde(default)]
    pub intermediate_nodes: Vec<TaxNode>,
    pub direct_parent: Option<TaxNode>,
    #[serde(default)]
    pub alternative_parents: Vec<TaxNode>,
    pub molecular_framework: Option<String>,
    #[serde(default)]
    pub substituents: Vec<String>,
    pub description: Option<String>,
    #[serde(default)]
    pub external_descriptors: Vec<ExternalDescriptor>,
    #[serde(default)]
    pub ancestors: Vec<String>,
    #[serde(default)]
    pub predicted_chebi_terms: Vec<String>,
    #[serde(default)]
    pub predicted_lipidmaps_terms: Vec<String>,
    pub classification_version: Option<String>,
}

impl EntityResponse {
    pub fn has_classification(&self) -> bool {
        self.direct_parent.is_some()
            || self.kingdom.is_some()
            || self.superclass.is_some()
            || self.class_node.is_some()
            || self.subclass.is_some()
    }

    pub fn taxonomy_labels(&self) -> Vec<(&'static str, String)> {
        let mut labels = Vec::new();
        if let Some(node) = &self.kingdom {
            labels.push(("kingdom", node.name.clone()));
        }
        if let Some(node) = &self.superclass {
            labels.push(("superclass", node.name.clone()));
        }
        if let Some(node) = &self.class_node {
            labels.push(("class", node.name.clone()));
        }
        if let Some(node) = &self.subclass {
            labels.push(("subclass", node.name.clone()));
        }
        if let Some(node) = &self.direct_parent {
            labels.push(("direct_parent", node.name.clone()));
        }
        labels
    }
}
