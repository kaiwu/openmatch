#ifndef OM_BUS_ERROR_H
#define OM_BUS_ERROR_H

/**
 * @file om_bus_error.h
 * @brief Error codes for the ombus message bus library
 *
 * Error code range: -800 to -899
 * Use om_bus_error_string() to get a human-readable description.
 */

typedef enum OmBusError {
    OM_ERR_BUS_INIT             = -800, /**< Bus initialization failed */
    OM_ERR_BUS_SHM_CREATE       = -801, /**< shm_open/ftruncate failed */
    OM_ERR_BUS_SHM_MAP          = -802, /**< mmap failed */
    OM_ERR_BUS_SHM_OPEN         = -803, /**< Consumer shm_open failed */
    OM_ERR_BUS_NOT_POW2         = -804, /**< Capacity not power of two */
    OM_ERR_BUS_CONSUMER_ID      = -805, /**< Invalid consumer index */
    OM_ERR_BUS_RECORD_TOO_LARGE = -806, /**< Payload exceeds slot_size - 24 */
    OM_ERR_BUS_MAGIC_MISMATCH   = -807, /**< SHM header magic mismatch */
    OM_ERR_BUS_VERSION_MISMATCH = -808, /**< SHM header version mismatch */
    OM_ERR_BUS_CRC_MISMATCH     = -809, /**< Payload CRC32 mismatch */
    OM_ERR_BUS_GAP_DETECTED     = -810, /**< WAL sequence gap detected */
    OM_ERR_BUS_EMPTY            = -811, /**< No record available */
} OmBusError;

/**
 * Get human-readable error string for bus error codes.
 *
 * @param err Error code
 * @return Static string describing the error
 */
static inline const char *om_bus_error_string(int err) {
    switch (err) {
        case 0:                          return "Success";
        case OM_ERR_BUS_INIT:            return "Bus initialization failed";
        case OM_ERR_BUS_SHM_CREATE:      return "SHM create failed";
        case OM_ERR_BUS_SHM_MAP:         return "SHM mmap failed";
        case OM_ERR_BUS_SHM_OPEN:        return "SHM open failed";
        case OM_ERR_BUS_NOT_POW2:        return "Capacity not power of 2";
        case OM_ERR_BUS_CONSUMER_ID:     return "Invalid consumer index";
        case OM_ERR_BUS_RECORD_TOO_LARGE: return "Record too large for slot";
        case OM_ERR_BUS_MAGIC_MISMATCH:  return "SHM magic mismatch";
        case OM_ERR_BUS_VERSION_MISMATCH: return "SHM version mismatch";
        case OM_ERR_BUS_CRC_MISMATCH:    return "Payload CRC32 mismatch";
        case OM_ERR_BUS_GAP_DETECTED:    return "WAL sequence gap detected";
        case OM_ERR_BUS_EMPTY:           return "No record available";
        default:                         return "Unknown bus error";
    }
}

#endif /* OM_BUS_ERROR_H */
