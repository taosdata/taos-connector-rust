use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use once_cell::sync::Lazy;

use crate::is_valid_host_port;

pub struct Cluster {
    addrs: Mutex<Vec<String>>,
}

impl Cluster {
    fn new(seeds: &[String]) -> Self {
        let mut addrs = Vec::new();
        for seed in seeds {
            if is_valid_host_port(seed) && !addrs.contains(seed) {
                addrs.push(seed.clone());
            }
        }
        Self {
            addrs: Mutex::new(addrs),
        }
    }

    fn addresses(&self) -> Vec<String> {
        self.addrs.lock().unwrap().clone()
    }

    fn add_addresses(&self, addrs: &[String]) {
        let mut cluster_addrs = self.addrs.lock().unwrap();
        for addr in addrs {
            if is_valid_host_port(addr) && !cluster_addrs.contains(addr) {
                cluster_addrs.push(addr.clone());
            }
        }
    }
}

pub struct ClusterRegistry {
    endpoint_to_cluster: Mutex<HashMap<String, Arc<Cluster>>>,
}

static REGISTRY: Lazy<ClusterRegistry> = Lazy::new(ClusterRegistry::new);

impl ClusterRegistry {
    pub fn global() -> &'static ClusterRegistry {
        &REGISTRY
    }

    pub fn new() -> Self {
        Self {
            endpoint_to_cluster: Mutex::new(HashMap::new()),
        }
    }

    pub fn update_cluster(&self, discovered: &[String]) {
        let valid = valid_unique(discovered);
        if valid.is_empty() {
            return;
        }

        let mut endpoint_to_cluster = self.endpoint_to_cluster.lock().unwrap();
        let clusters = find_clusters(&endpoint_to_cluster, &valid);
        let cluster = match clusters.as_slice() {
            [] => Arc::new(Cluster::new(&valid)),
            [cluster] => Arc::clone(cluster),
            _ => {
                tracing::warn!(
                    "adapter HA: discovered endpoints touch multiple known clusters, skip update: {:?}",
                    valid
                );
                return;
            }
        };

        cluster.add_addresses(&valid);
        for addr in &valid {
            endpoint_to_cluster.insert(addr.clone(), Arc::clone(&cluster));
        }
    }

    pub fn expand_endpoints(&self, seeds: &[String]) -> Vec<String> {
        let valid = valid_unique(seeds);
        if valid.is_empty() {
            return seeds.to_vec();
        }

        let endpoint_to_cluster = self.endpoint_to_cluster.lock().unwrap();
        let clusters = find_clusters(&endpoint_to_cluster, &valid);
        let cluster = match clusters.as_slice() {
            [cluster] => Arc::clone(cluster),
            [] => return seeds.to_vec(),
            _ => {
                tracing::warn!(
                    "adapter HA: seeds touch multiple known clusters, skip endpoint expansion: {:?}",
                    valid
                );
                return seeds.to_vec();
            }
        };
        drop(endpoint_to_cluster);

        let mut expanded = Vec::new();
        for seed in seeds {
            if !expanded.contains(seed) {
                expanded.push(seed.clone());
            }
        }
        for addr in cluster.addresses() {
            if !expanded.contains(&addr) {
                expanded.push(addr);
            }
        }
        expanded
    }
}

fn valid_unique(addrs: &[String]) -> Vec<String> {
    let mut valid = Vec::new();
    for addr in addrs {
        if is_valid_host_port(addr) && !valid.contains(addr) {
            valid.push(addr.clone());
        }
    }
    valid
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
            registry.expand_endpoints(&strings(&["ha-create-b.example:6041"])),
            strings(&["ha-create-b.example:6041", "ha-create-a.example:6041",])
        );
        assert_eq!(
            registry.expand_endpoints(&strings(&["ha-create-invalid"])),
            strings(&["ha-create-invalid"])
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
            registry.expand_endpoints(&strings(&[
                "ha-expand-b.example:6041",
                "ha-expand-d.example:6041",
            ])),
            strings(&[
                "ha-expand-b.example:6041",
                "ha-expand-d.example:6041",
                "ha-expand-a.example:6041",
                "ha-expand-c.example:6041",
            ])
        );

        assert_eq!(
            registry.expand_endpoints(&strings(&["ha-expand-unknown.example:6041"])),
            strings(&["ha-expand-unknown.example:6041"])
        );

        registry.update_cluster(&strings(&["ha-expand-other.example:6041"]));
        assert_eq!(
            registry.expand_endpoints(&strings(&[
                "ha-expand-a.example:6041",
                "ha-expand-other.example:6041",
            ])),
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
            registry.expand_endpoints(&strings(&["ha-update-b.example:6041"])),
            strings(&["ha-update-b.example:6041", "ha-update-a.example:6041"])
        );

        assert_eq!(
            registry.expand_endpoints(&strings(&["ha-update-invalid"])),
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
            registry.expand_endpoints(&strings(&["ha-multi-c.example:6041"])),
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
            registry.expand_endpoints(&strings(&["ha-transitive-c.example:6041"])),
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

        let expanded = registry.expand_endpoints(&strings(&["ha-concurrent-seed.example:6041"]));
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

        let expanded = registry.expand_endpoints(&strings(&["ha-concurrent-first-b.example:6041"]));
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
