//! Process-local extension publication and selection policy.
use crate::{LifecycleHooks, SessionError, Tool, invalid, wire};
use std::{collections::BTreeSet, rc::Rc};

/// A named bundle of trusted local code. Names are case-sensitive.
#[derive(Clone)]
pub struct Extension {
    name: String,
    tools: Vec<Tool>,
    hooks: LifecycleHooks,
}
impl Extension {
    pub fn new(name: impl Into<String>, tools: Vec<Tool>) -> Self {
        Self {
            name: name.into(),
            tools,
            hooks: LifecycleHooks::default(),
        }
    }
    pub fn with_hooks(mut self, hooks: LifecycleHooks) -> Self {
        self.hooks = hooks;
        self
    }
    pub fn hooks(&self) -> &LifecycleHooks {
        &self.hooks
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn tools(&self) -> &[Tool] {
        &self.tools
    }
    fn validate(&self) -> Result<(), SessionError> {
        if self.name.is_empty() {
            return Err(invalid("Empty extension name"));
        }
        let mut names = BTreeSet::new();
        for tool in &self.tools {
            let declaration = wire::declaration(tool.declaration());
            wire::decode_declaration(&declaration)?;
            wire::validate_entry(None, Some(&declaration))?;
            if !names.insert(&tool.declaration().name) {
                return Err(invalid(format!(
                    "Duplicate tool name in extension {}: {}",
                    self.name,
                    tool.declaration().name
                )));
            }
        }
        Ok(())
    }
}

/// Per-conversation extension selection. Missing names remain in the
/// configuration and are skipped during resolution.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum ExtensionSelection {
    /// Use the host default, or all installed extensions if it is absent.
    #[default]
    Default,
    /// Select exactly these names in order; an empty list selects nothing.
    Exact(Vec<String>),
    /// Remove from the host default, then append additions. An addition can
    /// reintroduce a removed name at the end. First occurrence wins.
    AddRemove {
        add: Vec<String>,
        remove: Vec<String>,
    },
}

/// Process-local extension registry. Snapshots are immutable clones, and
/// executable extensions must be installed again after restart.
///
/// ```
/// use publicworks_agent::{AgentRegistry, Extension, ExtensionSelection};
///
/// let mut registry = AgentRegistry::new();
/// registry.install(Extension::new("search", vec![]))?;
/// let snapshot = registry.snapshot();
/// let selection = ExtensionSelection::Exact(vec!["search".into(), "later".into()]);
/// let selected = snapshot.resolve(&selection, None);
/// assert_eq!(selected.extensions().len(), 1);
///
/// registry.uninstall("search")?;
/// assert!(registry.snapshot().get("search").is_none());
/// assert!(snapshot.get("search").is_some());
/// assert_eq!(selected.extensions()[0].name(), "search");
/// # Ok::<(), publicworks_runtime::SessionError>(())
/// ```
#[derive(Default)]
pub struct AgentRegistry {
    current: AgentRegistrySnapshot,
}
impl AgentRegistry {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn snapshot(&self) -> AgentRegistrySnapshot {
        self.current.clone()
    }
    /// Validates and publishes an extension. Replacement preserves its position;
    /// new names append, and errors leave the generation unchanged.
    pub fn install(&mut self, extension: Extension) -> Result<(), SessionError> {
        extension.validate()?;
        let mut extensions = self.current.0.extensions.clone();
        if let Some(position) = extensions.iter().position(|e| e.name() == extension.name()) {
            extensions[position] = Rc::new(extension);
        } else {
            extensions.push(Rc::new(extension));
        }
        self.publish(extensions)
    }
    /// Removes a name without cancelling existing work. Returns false if absent.
    pub fn uninstall(&mut self, name: &str) -> Result<bool, SessionError> {
        let Some(position) = self
            .current
            .0
            .extensions
            .iter()
            .position(|e| e.name() == name)
        else {
            return Ok(false);
        };
        let mut extensions = self.current.0.extensions.clone();
        extensions.remove(position);
        self.publish(extensions)?;
        Ok(true)
    }
    fn publish(&mut self, extensions: Vec<Rc<Extension>>) -> Result<(), SessionError> {
        let generation = self
            .current
            .generation()
            .checked_add(1)
            .ok_or_else(|| invalid("Agent registry generation exhausted"))?;
        self.current = AgentRegistrySnapshot(Rc::new(RegistryState {
            generation,
            extensions,
        }));
        Ok(())
    }
}

#[derive(Default)]
struct RegistryState {
    generation: u64,
    extensions: Vec<Rc<Extension>>,
}

/// An immutable publication retaining the original callback identities.
#[derive(Clone, Default)]
pub struct AgentRegistrySnapshot(Rc<RegistryState>);
impl AgentRegistrySnapshot {
    /// Process-local generation counter; it is not a durable identity.
    pub fn generation(&self) -> u64 {
        self.0.generation
    }
    pub fn extensions(&self) -> &[Rc<Extension>] {
        &self.0.extensions
    }
    pub fn get(&self, name: &str) -> Option<&Rc<Extension>> {
        self.extensions().iter().find(|e| e.name() == name)
    }
    /// Resolves a selection without callbacks or mutation. Missing names are
    /// skipped. Selected extensions preserve first occurrence; later same-name
    /// tools replace earlier tools in place.
    pub fn resolve(
        &self,
        selection: &ExtensionSelection,
        host_default: Option<&[String]>,
    ) -> ResolvedExtensions {
        let default_names = || -> Vec<&str> {
            match host_default {
                Some(names) => names.iter().map(String::as_str).collect(),
                None => self.extensions().iter().map(|e| e.name()).collect(),
            }
        };
        let names = match selection {
            ExtensionSelection::Default => default_names(),
            ExtensionSelection::Exact(names) => names.iter().map(String::as_str).collect(),
            ExtensionSelection::AddRemove { add, remove } => {
                let removed: BTreeSet<_> = remove.iter().map(String::as_str).collect();
                let mut names = default_names();
                names.retain(|name| !removed.contains(name));
                names.extend(add.iter().map(String::as_str));
                names
            }
        };
        let mut seen = BTreeSet::new();
        let extensions: Vec<_> = names
            .into_iter()
            .filter(|name| seen.insert(*name))
            .filter_map(|name| self.get(name).cloned())
            .collect();
        let mut tools: Vec<Tool> = Vec::new();
        for extension in &extensions {
            for tool in extension.tools() {
                if let Some(position) = tools
                    .iter()
                    .position(|t| t.declaration().name == tool.declaration().name)
                {
                    tools[position] = tool.clone();
                } else {
                    tools.push(tool.clone());
                }
            }
        }
        ResolvedExtensions { extensions, tools }
    }
}

/// An owned resolution that remains valid after registry replacement or removal.
#[derive(Clone)]
pub struct ResolvedExtensions {
    extensions: Vec<Rc<Extension>>,
    tools: Vec<Tool>,
}
impl ResolvedExtensions {
    pub fn extensions(&self) -> &[Rc<Extension>] {
        &self.extensions
    }
    pub fn tools(&self) -> &[Tool] {
        &self.tools
    }
}

#[cfg(test)]
mod tests;
