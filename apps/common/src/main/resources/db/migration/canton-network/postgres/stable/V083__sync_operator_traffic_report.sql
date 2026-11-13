-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Traffic charged to the participants of the synchronizer a sync operator app operates,
-- per 10-minute interval of record time.
create table sync_operator_traffic_report
(
    -- Start of the interval in micros since the epoch, a multiple of 10 minutes.
    interval_start   bigint not null primary key,
    -- Traffic charged to the participants in the interval, in bytes.
    traffic_consumed bigint not null
);
