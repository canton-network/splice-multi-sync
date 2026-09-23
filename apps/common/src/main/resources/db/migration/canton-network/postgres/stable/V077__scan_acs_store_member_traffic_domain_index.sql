-- The member-traffic index leads with the member, so a total for one synchronizer across all its
-- members would read every member's rows. This one leads with the synchronizer. It coexists with
-- the other rather than replacing it: the per-member query still needs the member first.
create index scan_acs_store_sid_mid_pn_tid_mtd
    on scan_acs_store (store_id, migration_id, package_name, template_id_qualified_name,
                       member_traffic_domain)
    where member_traffic_domain is not null;
