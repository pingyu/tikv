// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

define_error_codes!(
    "KV:CausalTs:",

    PD => ("PdClient", "", ""),
    TSO => ("TSO", "", ""),

    UNKNOWN => ("Unknown", "", "")
);
