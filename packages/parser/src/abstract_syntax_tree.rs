use crate::{
    attributes::AttributeDeclarationNode, modules::ModuleDeclarationNode,
    providers::ProviderDeclarationNode, vars::VarDeclarationNode,
};
use nv_lexer::{
    tokens::{LexerLiteral, TokenRange},
    LexerType, LexerVarModifierKeyword,
};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, PartialEq)]
pub struct Leaf<V> {
    pub value: V,
    pub range: TokenRange,
}
impl<V> Leaf<V> {
    pub fn new(value: V, range: TokenRange) -> Self {
        Self { value, range }
    }
}

pub type Identifier = Leaf<String>;
pub type Literal = Leaf<LexerLiteral>;
pub type Type = Leaf<LexerType>;
pub type ProviderType = Leaf<String>;
pub type Modifier = Leaf<LexerVarModifierKeyword>;

#[derive(Debug, Clone)]
pub enum DeclarationNode {
    VarDeclaration(Arc<VarDeclarationNode>),
    ModuleDeclaration(ModuleDeclarationNode),
    ProviderDeclaration(ProviderDeclarationNode),
    AttributeDeclaration(AttributeDeclarationNode),
}

impl From<AbstractSyntaxNode> for DeclarationNode {
    fn from(declaration: AbstractSyntaxNode) -> Self {
        if let AbstractSyntaxNode::Declaration(DeclarationNode::VarDeclaration(var)) = declaration {
            return var.as_ref().clone().into();
        }

        panic!("Invalid conversion");
    }
}

impl From<VarDeclarationNode> for DeclarationNode {
    fn from(declaration: VarDeclarationNode) -> Self {
        DeclarationNode::VarDeclaration(Arc::new(declaration))
    }
}

#[derive(Debug)]
pub struct SourceFileNode {
    pub declarations: Mutex<Vec<Arc<DeclarationNode>>>,
}

#[derive(Debug, Clone)]
pub enum AbstractSyntaxNode {
    SourceFile(Arc<SourceFileNode>),
    Declaration(DeclarationNode),
}

#[derive(Debug)]
pub struct AbstractSyntaxTree {
    pub root: Option<AbstractSyntaxNode>,
}
