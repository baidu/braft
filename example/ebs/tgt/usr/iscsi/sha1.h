/*
 * sha1.h - SHA1 Secure Hash Algorithm used for CHAP authentication.
 * copied from the Linux kernel's Cryptographic API and slightly adjusted to
 * fit IET's needs
 *
 * This file is (c) 2004 Xiranet Communications GmbH <arne.redlich@xiranet.com>
 * and licensed under the GPL.
 */

#ifndef SHA1_H
#define SHA1_H

#include <sys/types.h>
#include <string.h>
#include <inttypes.h>

struct sha1_ctx {
        uint64_t count;
        uint32_t state[5];
        uint8_t buffer[64];
};

void sha1_init(void *ctx);
void sha1_update(void *ctx, const uint8_t *data, unsigned int len);
void sha1_final(void* ctx, uint8_t *out);

#endif
