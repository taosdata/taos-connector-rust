// Copyright (c) 2026 TAOS Data, Inc.
//
// SPDX-License-Identifier: MIT
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use once_cell::sync::Lazy;

use crate::is_valid_host_port;

pub struct Cluster {
    addrs: Mutex<Vec<String>>,
}

impl Cluster {
    fn new(seeds: &[String]) -> Self {
        Self {
            addrs: Mutex::new(seeds.to_vec()),
        }
    }

    fn addresses(&self) -> Vec<String> {
        self.addrs.lock().unwrap().clone()
    }

    fn add_addresses(&self, addrs: &[String]) -> bool {
        let mut cluster_addrs = self.addrs.lock().unwrap();
        let new_addrs: Vec<String> = {
            let mut seen: HashSet<&str> = cluster_addrs.iter().map(String::as_str).collect();
            addrs
                .iter()
                .filter(|addr| seen.insert(addr.as_str()))
                .cloned()
                .collect()
        };
        let changed = !new_addrs.is_empty();
        cluster_addrs.extend(new_addrs);
        changed
    }
}

pub struct ClusterRegistry {
    endpoint_to_cluster: Mutex<HashMap<String, Arc<Cluster>>>,
    version: AtomicUsize,
}

static REGISTRY: Lazy<ClusterRegistry> = Lazy::new(ClusterRegistry::new);

impl ClusterRegistry {
    pub fn global() -> &'static ClusterRegistry {
        &REGISTRY
    }

    pub fn new() -> Self {
        Self {
            endpoint_to_cluster: Mutex::new(HashMap::new()),
            version: AtomicUsize::new(0),
        }
    }

    pub fn version(&self) -> usize {
        self.version.load(Ordering::Acquire)
    }

    pub fn update_cluster(&self, discovered: &[String]) -> usize {
        let valid = valid_unique(discovered);
        if valid.is_empty() {
            return self.version();
        }

        let mut endpoint_to_cluster = self.endpoint_to_cluster.lock().unwrap();
        let clusters = find_clusters(&endpoint_to_cluster, &valid);
        let (cluster, changed) = match clusters.as_slice() {
            [] => (Arc::new(Cluster::new(&valid)), true),
            [cluster] => {
                let cluster = Arc::clone(cluster);
                let changed = cluster.add_addresses(&valid);
                (cluster, changed)
            }
            _ => {
                tracing::warn!(
                    "adapter HA: discovered endpoints touch multiple known clusters, skip update: {:?}",
                    valid
                );
                return self.version();
            }
        };

        for addr in &valid {
            endpoint_to_cluster.insert(addr.clone(), Arc::clone(&cluster));
        }

        if changed {
            self.version.fetch_add(1, Ordering::AcqRel) + 1
        } else {
            self.version()
        }
    }

    pub fn expand_endpoints_if_changed(
        &self,
        seeds: &[String],
        known_version: usize,
    ) -> Option<(usize, Vec<String>)> {
        let expanded = self.expand_endpoints_with_version(seeds);
        (expanded.0 != known_version).then_some(expanded)
    }

    pub fn expand_endpoints_with_version(&self, seeds: &[String]) -> (usize, Vec<String>) {
        let valid = valid_unique(seeds);
        if valid.is_empty() {
            return (self.version(), seeds.to_vec());
        }

        let endpoint_to_cluster = self.endpoint_to_cluster.lock().unwrap();
        let version = self.version();
        let clusters = find_clusters(&endpoint_to_cluster, &valid);
        let cluster = match clusters.as_slice() {
            [cluster] => Arc::clone(cluster),
            [] => return (version, seeds.to_vec()),
            _ => {
                tracing::warn!(
                    "adapter HA: seeds touch multiple known clusters, skip endpoint expansion: {:?}",
                    valid
                );
                return (version, seeds.to_vec());
            }
        };
        drop(endpoint_to_cluster);

        let cluster_addrs = cluster.addresses();
        let mut expanded = Vec::with_capacity(seeds.len() + cluster_addrs.len());
        let mut seen = HashSet::with_capacity(seeds.len() + cluster_addrs.len());
        for seed in seeds {
            if seen.insert(seed.as_str()) {
                expanded.push(seed.clone());
            }
        }
        for addr in &cluster_addrs {
            if seen.insert(addr.as_str()) {
                expanded.push(addr.clone());
            }
        }
        (version, expanded)
    }
}

fn valid_unique(addrs: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    addrs
        .iter()
        .filter(|addr| is_valid_host_port(addr) && seen.insert(addr.as_str()))
        .cloned()
        .collect()
}

fn find_clusters(
    endpoint_to_cluster: &HashMap<String, Arc<Cluster>>,
    addrs: &[String],
) -> Vec<Arc<Cluster>> {
    let mut clusters: Vec<Arc<Cluster>> = Vec::new();
    for addr in addrs {
        let Some(cluster) = endpoint_to_cluster.get(addr) else {
            continue;
        };
        if !clusters.iter().any(|known| Arc::ptr_eq(known, cluster)) {
            clusters.push(Arc::clone(cluster));
        }
    }
    clusters
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use super::ClusterRegistry;

    fn strings(addrs: &[&str]) -> Vec<String> {
        addrs.iter().map(|addr| addr.to_string()).collect()
    }

    #[test]
    fn test_update_cluster_creates_cluster_when_no_match() {
        let registry = ClusterRegistry::new();

        registry.update_cluster(&strings(&[
            "ha-create-a.example:6041",
            "ha-create-b.example:6041",
            "ha-create-b.example:6041",
            "ha-create-invalid",
            "ha-create-zero.example:0",
        ]));

        assert_eq!(
            registry
                .expand_endpoints_with_version(&strings(&["ha-create-b.example:6041"]))
                .1,
            strings(&["ha-create-b.example:6041", "ha-create-a.example:6041",])
        );
        assert_eq!(
            registry
                .expand_endpoints_with_version(&strings(&["ha-create-invalid"]))
                .1,
            strings(&["ha-create-invalid"])
        );
    }

    #[test]
    fn test_registry_version_changes_only_when_topology_changes() {
        let registry = ClusterRegistry::new();
        let seed = strings(&["ha-version-a.example:6041"]);

        assert_eq!(registry.version(), 0);

        let first_version = registry.update_cluster(&seed);
        assert!(first_version > 0);
        assert_eq!(registry.version(), first_version);
        assert!(registry
            .expand_endpoints_if_changed(&seed, first_version)
            .is_none());

        assert_eq!(registry.update_cluster(&seed), first_version);

        let second_version = registry.update_cluster(&strings(&[
            "ha-version-a.example:6041",
            "ha-version-b.example:6041",
        ]));
        assert!(second_version > first_version);

        let (expanded_version, expanded) = registry
            .expand_endpoints_if_changed(&seed, first_version)
            .expect("new topology version should trigger endpoint expansion");
        assert_eq!(expanded_version, second_version);
        assert_eq!(
            expanded,
            strings(&["ha-version-a.example:6041", "ha-version-b.example:6041",])
        );
    }

    #[test]
    fn test_expand_endpoints_returns_seed_union_known_cluster_addresses_in_stable_order() {
        let registry = ClusterRegistry::new();
        registry.update_cluster(&strings(&[
            "ha-expand-a.example:6041",
            "ha-expand-b.example:6041",
            "ha-expand-c.example:6041",
        ]));

        assert_eq!(
            registry
                .expand_endpoints_with_version(&strings(&[
                    "ha-expand-b.example:6041",
                    "ha-expand-d.example:6041",
                ]))
                .1,
            strings(&[
                "ha-expand-b.example:6041",
                "ha-expand-d.example:6041",
                "ha-expand-a.example:6041",
                "ha-expand-c.example:6041",
            ])
        );

        assert_eq!(
            registry
                .expand_endpoints_with_version(&strings(&["ha-expand-unknown.example:6041"]))
                .1,
            strings(&["ha-expand-unknown.example:6041"])
        );

        registry.update_cluster(&strings(&["ha-expand-other.example:6041"]));
        assert_eq!(
            registry
                .expand_endpoints_with_version(&strings(&[
                    "ha-expand-a.example:6041",
                    "ha-expand-other.example:6041",
                ]))
                .1,
            strings(&["ha-expand-a.example:6041", "ha-expand-other.example:6041",])
        );
    }

    #[test]
    fn test_update_cluster_adds_and_indexes_new_valid_addresses() {
        let registry = ClusterRegistry::new();
        registry.update_cluster(&strings(&["ha-update-a.example:6041"]));

        registry.update_cluster(&strings(&[
            "ha-update-a.example:6041",
            "ha-update-b.example:6041",
            "ha-update-b.example:6041",
            "ha-update-invalid",
            "ha-update-zero.example:0",
        ]));

        assert_eq!(
            registry
                .expand_endpoints_with_version(&strings(&["ha-update-b.example:6041"]))
                .1,
            strings(&["ha-update-b.example:6041", "ha-update-a.example:6041"])
        );

        assert_eq!(
            registry
                .expand_endpoints_with_version(&strings(&["ha-update-invalid"]))
                .1,
            strings(&["ha-update-invalid"])
        );
    }

    #[test]
    fn test_update_cluster_touching_multiple_clusters_is_skipped() {
        let registry = ClusterRegistry::new();
        registry.update_cluster(&strings(&["ha-multi-a.example:6041"]));
        registry.update_cluster(&strings(&["ha-multi-b.example:6041"]));

        registry.update_cluster(&strings(&[
            "ha-multi-a.example:6041",
            "ha-multi-b.example:6041",
            "ha-multi-c.example:6041",
        ]));

        assert_eq!(
            registry
                .expand_endpoints_with_version(&strings(&["ha-multi-c.example:6041"]))
                .1,
            strings(&["ha-multi-c.example:6041"])
        );
    }

    #[test]
    fn test_cluster_identity_is_transitive_through_discovered_endpoints() {
        let registry = ClusterRegistry::new();
        registry.update_cluster(&strings(&[
            "ha-transitive-a.example:6041",
            "ha-transitive-b.example:6041",
        ]));

        registry.update_cluster(&strings(&[
            "ha-transitive-b.example:6041",
            "ha-transitive-c.example:6041",
        ]));

        assert_eq!(
            registry
                .expand_endpoints_with_version(&strings(&["ha-transitive-c.example:6041"]))
                .1,
            strings(&[
                "ha-transitive-c.example:6041",
                "ha-transitive-a.example:6041",
                "ha-transitive-b.example:6041",
            ])
        );
    }

    #[test]
    fn test_concurrent_updates_keep_the_union_without_duplicates() {
        let registry = Arc::new(ClusterRegistry::new());
        registry.update_cluster(&strings(&["ha-concurrent-seed.example:6041"]));

        let handles: Vec<_> = (0..8)
            .map(|idx| {
                let registry = Arc::clone(&registry);
                thread::spawn(move || {
                    registry.update_cluster(&strings(&[
                        "ha-concurrent-seed.example:6041",
                        &format!("ha-concurrent-{idx}.example:6041"),
                    ]));
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let expanded = registry
            .expand_endpoints_with_version(&strings(&["ha-concurrent-seed.example:6041"]))
            .1;
        assert_eq!(expanded.len(), 9);
        assert_eq!(expanded[0], "ha-concurrent-seed.example:6041");
        for idx in 0..8 {
            let addr = format!("ha-concurrent-{idx}.example:6041");
            assert_eq!(
                expanded.iter().filter(|item| *item == &addr).count(),
                1,
                "{addr} should be present once"
            );
        }
    }

    #[test]
    fn test_concurrent_first_updates_from_different_seeds_do_not_split_cluster() {
        let registry = Arc::new(ClusterRegistry::new());

        let handles: Vec<_> = ["a", "b"]
            .into_iter()
            .map(|_| {
                let registry = Arc::clone(&registry);
                thread::spawn(move || {
                    registry.update_cluster(&strings(&[
                        "ha-concurrent-first-a.example:6041",
                        "ha-concurrent-first-b.example:6041",
                        "ha-concurrent-first-c.example:6041",
                    ]));
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }

        let expanded = registry
            .expand_endpoints_with_version(&strings(&["ha-concurrent-first-b.example:6041"]))
            .1;
        assert_eq!(
            expanded,
            strings(&[
                "ha-concurrent-first-b.example:6041",
                "ha-concurrent-first-a.example:6041",
                "ha-concurrent-first-c.example:6041",
            ])
        );
    }
}
