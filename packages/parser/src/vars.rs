use serde::Serialize;

use crate::{
    abstract_syntax_tree::{Identifier, Modifier, Type},
    attributes::{AttributeDeclarationNode, PartialAttributeDeclarationNode},
    AbstractSyntaxNode,
};
use std::{sync::Arc, sync::Weak};

#[derive(Debug, Clone, Serialize)]
pub struct VarDeclarationNode {
    pub identifier: Identifier,
    pub type_value: Type,
    pub modifier: Option<Modifier>,
    pub attributes: Vec<Arc<AttributeDeclarationNode>>,
    #[serde(skip_serializing)]
    pub parent: Weak<AbstractSyntaxNode>,
}

impl PartialEq for VarDeclarationNode {
    fn eq(&self, other: &Self) -> bool {
        self.identifier == other.identifier
            && self.type_value == other.type_value
            && self.modifier == other.modifier
            && self.attributes == other.attributes
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PartialVarDeclarationNode {
    pub identifier: Option<Identifier>,
    pub type_value: Option<Type>,
    pub modifier: Option<Modifier>,
    pub attributes: Vec<PartialAttributeDeclarationNode>,
    #[serde(skip_serializing)]
    pub parent: Weak<AbstractSyntaxNode>,
}

impl TryFrom<PartialVarDeclarationNode> for VarDeclarationNode {
    type Error = ();

    fn try_from(partial: PartialVarDeclarationNode) -> Result<Self, Self::Error> {
        if partial.identifier.is_none() || partial.type_value.is_none() {
            return Err(());
        }

        Ok(VarDeclarationNode {
            parent: partial.parent,
            identifier: partial.identifier.unwrap(),
            type_value: partial.type_value.unwrap(),
            modifier: partial.modifier,
            attributes: partial
                .attributes
                .into_iter()
                .filter_map(|attribute| attribute.try_into().ok())
                .collect(),
        })
    }
}
