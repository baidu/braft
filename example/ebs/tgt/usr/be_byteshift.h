#ifndef _LINUX_UNALIGNED_BE_BYTESHIFT_H
#define _LINUX_UNALIGNED_BE_BYTESHIFT_H

static inline uint16_t __get_unaligned_be16(const uint8_t *p)
{
	return p[0] << 8 | p[1];
}

static inline uint32_t __get_unaligned_be32(const uint8_t *p)
{
	return p[0] << 24 | p[1] << 16 | p[2] << 8 | p[3];
}

static inline uint64_t __get_unaligned_be64(const uint8_t *p)
{
	return (uint64_t)__get_unaligned_be32(p) << 32 |
	       __get_unaligned_be32(p + 4);
}

static inline void __put_unaligned_be16(uint16_t val, uint8_t *p)
{
	*p++ = val >> 8;
	*p++ = val;
}

static inline void __put_unaligned_be32(uint32_t val, uint8_t *p)
{
	__put_unaligned_be16(val >> 16, p);
	__put_unaligned_be16(val, p + 2);
}

static inline void __put_unaligned_be64(uint64_t val, uint8_t *p)
{
	__put_unaligned_be32(val >> 32, p);
	__put_unaligned_be32(val, p + 4);
}

static inline uint16_t get_unaligned_be16(const void *p)
{
	return __get_unaligned_be16((const uint8_t *)p);
}

static inline uint32_t get_unaligned_be24(const uint8_t *p)
{
	return p[0] << 16 | p[1] << 8 | p[2];
}

static inline uint32_t get_unaligned_be32(const void *p)
{
	return __get_unaligned_be32((const uint8_t *)p);
}

static inline uint64_t get_unaligned_be64(const void *p)
{
	return __get_unaligned_be64((const uint8_t *)p);
}

static inline void put_unaligned_be16(uint16_t val, void *p)
{
	__put_unaligned_be16(val, p);
}

static inline void put_unaligned_be24(uint32_t val, void *p)
{
	((uint8_t *)p)[0] = (val >> 16) & 0xff;
	((uint8_t *)p)[1] = (val >> 8) & 0xff;
	((uint8_t *)p)[2] = val & 0xff;
}

static inline void put_unaligned_be32(uint32_t val, void *p)
{
	__put_unaligned_be32(val, p);
}

static inline void put_unaligned_be64(uint64_t val, void *p)
{
	__put_unaligned_be64(val, p);
}

#endif /* _LINUX_UNALIGNED_BE_BYTESHIFT_H */
