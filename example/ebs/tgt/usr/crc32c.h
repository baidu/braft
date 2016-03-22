#ifndef _LINUX_CRC32C_H
#define _LINUX_CRC32C_H

#include <stdlib.h>
#include <stdint.h>

extern uint32_t crc32c_le(uint32_t crc, unsigned char const *address, size_t length);
extern uint32_t crc32c_be(uint32_t crc, unsigned char const *address, size_t length);

#define crc32c(seed, data, length)  crc32c_le(seed, (unsigned char const *)data, length)

#endif	/* _LINUX_CRC32C_H */
