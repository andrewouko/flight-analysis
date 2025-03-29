from dataclasses import dataclass
from enum import Enum


class FlightType(Enum):
    A = "Additional - Cargo/Mail"
    B = "Additional - Passenger - Shuttle Mode"
    C = "Charter - Passenger only"
    D = "Other - General Aviation"
    E = "Other - Special (FAA/Government)"
    F = "Scheduled - Cargo/Mail - Loose loaded cargo and/or preloaded devices"
    G = "Additional - Passenger - Normal Service"
    H = "Charter - Cargo and/or Mail"
    J = "Scheduled - Passenger - Normal Service"
    K = "Other - Training (school/crew check)"
    L = "Charter - Passenger and Cargo and/or Mail"
    M = "Scheduled - Cargo/Mail - Mail only"
    O = "Charter - Charter requiring special handling"
    P = "Other - Non-Revenue (Positioning/Ferry/Delivery/Demo)"
    Q = "Scheduled - Passenger/Cargo in Cabin (pax)"
    R = "Additional - Passenger/Cargo in Cabin (pax cum freighter)"
    S = "Scheduled - Passenger - Shuttle Mode"
    T = "Other - Technical Test"
    U = "Scheduled - Passenger - Service Operated by Surface Vehicle"
    V = "Scheduled - Cargo/Mail - Service Operated by Surface Vehicle"
    W = "Other - Military"
    X = "Other - Technical Stop"


@dataclass(frozen=True)
class Config:
    """
    Configuration for running Eduardo pipeline.
    
    Controls various aspects of the pipeline execution including offline mode,
    database interactions, file handling, and optimization settings.
    """
    o: bool = False
    """Offline mode: If True, reads results from local files instead of making API calls"""
    
    d: bool = False
    """Database skip: If True, skips updating the database with processed results"""
    
    c: bool = False
    """Cleanup skip: If True, preserves temporary files that would normally be deleted"""
    
    b: bool = False
    """Batching disable: If True, disables the batching of queries for optimization"""
    
    s: bool = False
    """Save skip: If True, skips saving results to CSV and Parquet files"""