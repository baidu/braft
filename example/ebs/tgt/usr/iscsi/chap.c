/*
 * chap.c - support for (mutual) CHAP authentication.
 * (C) 2004 Xiranet Communications GmbH <arne.redlich@xiranet.com>
 *
 * heavily based on code from iscsid.c:
 *   Copyright (C) 2002-2003 Ardis Technolgies <roman@ardistech.com>,
 *
 * and code taken from UNH iSCSI software:
 *   Copyright (C) 2001-2003 InterOperability Lab (IOL)
 *   University of New Hampshire (UNH)
 *   Durham, NH 03824
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, version 2 of the
 * License.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "iscsid.h"
#include "tgtd.h"
#include "md5.h"
#include "sha1.h"

#define HEX_FORMAT    0x01
#define BASE64_FORMAT 0x02

#define CHAP_DIGEST_ALG_MD5   5
#define CHAP_DIGEST_ALG_SHA1  7

#define CHAP_MD5_DIGEST_LEN  16
#define CHAP_SHA1_DIGEST_LEN 20

#define CHAP_INITIATOR_ERROR -1
#define CHAP_AUTH_ERROR      -2
#define CHAP_TARGET_ERROR    -3

#define CHAP_AUTH_STATE_START     AUTH_STATE_START
#define CHAP_AUTH_STATE_CHALLENGE 1
#define CHAP_AUTH_STATE_RESPONSE  2

#define CHAP_INITIATOR_AUTH 0
#define CHAP_TARGET_AUTH    1

#define CHAP_CHALLENGE_MAX	50

static inline int decode_hex_digit(char c)
{
	switch (c) {
	case '0' ... '9':
		return c - '0';
	case 'a' ... 'f':
		return c - 'a' + 10;
	case 'A' ... 'F':
		return c - 'A' + 10;
	}
	return 0;
}

static void decode_hex_string(char *hex_string, uint8_t *intnum, int intlen)
{
	char *ptr;
	int j;

	j = strlen(hex_string);
	ptr = hex_string + j;
	j = --intlen;
	do {
		intnum[j] = decode_hex_digit(*--ptr);
		intnum[j] |= decode_hex_digit(*--ptr) << 4;
		j--;
	} while (ptr > hex_string);

	while (j >= 0)
		intnum[j--] = 0;
}


/* Base64 decoding, taken from UNH-iSCSI "Base64codeToNumber()" */
static uint8_t decode_base64_digit(char base64)
{
	switch (base64) {
	case '=':
		return 64;
	case '/':
		return 63;
	case '+':
		return 62;
	default:
		if ((base64 >= 'A') && (base64 <= 'Z'))
			return base64 - 'A';
		else if ((base64 >= 'a') && (base64 <= 'z'))
			return 26 + (base64 - 'a');
		else if ((base64 >= '0') && (base64 <= '9'))
			return 52 + (base64 - '0');
		else
			return -1;
	}
}

/* Base64 decoding, taken from UNH-iSCSI "Base64StringToInteger()" */
static void decode_base64_string(char *string, uint8_t *intnum, int int_len)
{
	int len;
	int count;
	int intptr;
	uint8_t num[4];
	int octets;

	if ((string == NULL) || (intnum == NULL))
		return;
	len = strlen(string);
	if (len == 0)
		return;
	if ((len % 4) != 0)
		return;
	count = 0;
	intptr = 0;
	while (count < len - 4) {
		num[0] = decode_base64_digit(string[count]);
		num[1] = decode_base64_digit(string[count + 1]);
		num[2] = decode_base64_digit(string[count + 2]);
		num[3] = decode_base64_digit(string[count + 3]);
		if ((num[0] == 65) || (num[1] == 65) || (num[2] == 65) || (num[3] == 65))
			return;
		count += 4;
		octets =
		    (num[0] << 18) | (num[1] << 12) | (num[2] << 6) | num[3];
		intnum[intptr] = (octets & 0xFF0000) >> 16;
		intnum[intptr + 1] = (octets & 0x00FF00) >> 8;
		intnum[intptr + 2] = octets & 0x0000FF;
		intptr += 3;
	}
	num[0] = decode_base64_digit(string[count]);
	num[1] = decode_base64_digit(string[count + 1]);
	num[2] = decode_base64_digit(string[count + 2]);
	num[3] = decode_base64_digit(string[count + 3]);
	if ((num[0] == 64) || (num[1] == 64))
		return;
	if (num[2] == 64) {
		if (num[3] != 64)
			return;
		intnum[intptr] = (num[0] << 2) | (num[1] >> 4);
	} else if (num[3] == 64) {
		intnum[intptr] = (num[0] << 2) | (num[1] >> 4);
		intnum[intptr + 1] = (num[1] << 4) | (num[2] >> 2);
	} else {
		octets =
		    (num[0] << 18) | (num[1] << 12) | (num[2] << 6) | num[3];
		intnum[intptr] = (octets & 0xFF0000) >> 16;
		intnum[intptr + 1] = (octets & 0x00FF00) >> 8;
		intnum[intptr + 2] = octets & 0x0000FF;
	}
}

static inline void encode_hex_string(uint8_t *intnum, long length, char *string)
{
	int i;
	char *strptr;

	strptr = string;
	for (i = 0; i < length; i++, strptr += 2)
			sprintf(strptr, "%.2hhx", intnum[i]);
}

/* Base64 encoding, taken from UNH iSCSI "IntegerToBase64String()" */
static void encode_base64_string(uint8_t *intnum, long length, char *string)
{
	int count, octets, strptr, delta;
	static const char base64code[] = { 'A', 'B', 'C', 'D', 'E', 'F', 'G',
					   'H', 'I', 'J', 'K', 'L', 'M', 'N',
					   'O', 'P', 'Q', 'R', 'S', 'T', 'U',
					   'V', 'W', 'X', 'Y', 'Z', 'a', 'b',
					   'c', 'd', 'e', 'f', 'g', 'h', 'i',
					   'j', 'k', 'l', 'm', 'n', 'o', 'p',
					   'q', 'r', 's', 't', 'u', 'v', 'w',
					   'x', 'y', 'z', '0', '1', '2', '3',
					   '4', '5', '6', '7', '8', '9', '+',
					   '/', '=' };

	if ((!intnum) || (!string) || (!length))
		return;

	count = 0;
	octets = 0;
	strptr = 0;

	while ((delta = (length - count)) > 2) {
		octets = (intnum[count] << 16) | (intnum[count + 1] << 8) | intnum[count + 2];
		string[strptr] = base64code[(octets & 0xfc0000) >> 18];
		string[strptr + 1] = base64code[(octets & 0x03f000) >> 12];
		string[strptr + 2] = base64code[(octets & 0x000fc0) >> 6];
		string[strptr + 3] = base64code[octets & 0x00003f];
		count += 3;
		strptr += 4;
	}
	if (delta == 1) {
		string[strptr] = base64code[(intnum[count] & 0xfc) >> 2];
		string[strptr + 1] = base64code[(intnum[count] & 0x03) << 4];
		string[strptr + 2] = base64code[64];
		string[strptr + 3] = base64code[64];
		strptr += 4;
	} else if (delta == 2) {
		string[strptr] = base64code[(intnum[count] & 0xfc) >> 2];
		string[strptr + 1] = base64code[((intnum[count] & 0x03) << 4) | ((intnum[count + 1] & 0xf0) >> 4)];
		string[strptr + 2] = base64code[(intnum[count + 1] & 0x0f) << 2];
		string[strptr + 3] = base64code[64];
		strptr += 4;
	}
	string[strptr] = '\0';
}

static inline int chap_check_encoding_format(char *encoded)
{
	int encoding_fmt;

	if (!encoded)
		return -1;
	if ((strlen(encoded) < 3) || (encoded[0] != '0'))
		return -1;

	if (encoded[1] == 'x' || encoded[1] == 'X')
		encoding_fmt = HEX_FORMAT;
	else if (encoded[1] == 'b' || encoded[1] == 'B')
		encoding_fmt = BASE64_FORMAT;
	else
		return -1;

	return encoding_fmt;
}

static int chap_alloc_decode_buffer(char *encoded, uint8_t **decode_buf, int encoding_fmt)
{
	int i;
	int decode_len = 0;

	i = strlen(encoded);
	i -= 2;

	if (encoding_fmt == HEX_FORMAT)
		decode_len = (i - 1) / 2 + 1;
	else if (encoding_fmt == BASE64_FORMAT) {
		if (i % 4)
			return CHAP_INITIATOR_ERROR;

		decode_len =  i / 4 * 3;
		if (encoded[i + 1] == '=')
			decode_len--;
		if (encoded[i] == '=')
			decode_len--;
	}

	if (!decode_len)
		return CHAP_INITIATOR_ERROR;

	*decode_buf = malloc(decode_len);
	if (!*decode_buf)
		return CHAP_TARGET_ERROR;

	return decode_len;
}

static int chap_decode_string(char *encoded, uint8_t *decode_buf, int buf_len, int encoding_fmt)
{
	if (encoding_fmt == HEX_FORMAT) {
		if ((strlen(encoded) - 2) > (2 * buf_len)) {
			eprintf("buf[%d] !sufficient to decode string[%d]\n",
				  buf_len, (int) strlen(encoded));
			return CHAP_TARGET_ERROR;
		}
		decode_hex_string(encoded + 2, decode_buf, buf_len);

	} else if (encoding_fmt == BASE64_FORMAT) {
		if ((strlen(encoded) - 2) > ((buf_len - 1) / 3 + 1) * 4) {
			eprintf("buf[%d] !sufficient to decode string[%d]\n",
				buf_len, (int) strlen(encoded));
			return CHAP_TARGET_ERROR;
		}
		decode_base64_string(encoded + 2, decode_buf, buf_len);

	} else
		return CHAP_INITIATOR_ERROR;

	return 0;
}

static inline void chap_encode_string(uint8_t *intnum, int buf_len, char *encode_buf, int encoding_fmt)
{
	encode_buf[0] = '0';
	if (encoding_fmt == HEX_FORMAT) {
		encode_buf[1] = 'x';
		encode_hex_string(intnum, buf_len, encode_buf + 2);
	} else if (encoding_fmt == BASE64_FORMAT) {
		encode_buf[1] = 'b';
		encode_base64_string(intnum, buf_len, encode_buf + 2);
	}
}

static inline void chap_calc_digest_md5(char chap_id, char *secret, int secret_len, uint8_t *challenge, int challenge_len, uint8_t *digest)
{
	struct MD5Context ctx;

	MD5Init(&ctx);
	MD5Update(&ctx, (unsigned char*)&chap_id, 1);
	MD5Update(&ctx, (unsigned char*)secret, secret_len);
	MD5Update(&ctx, challenge, challenge_len);
	MD5Final(digest, &ctx);
}

static inline void chap_calc_digest_sha1(char chap_id, char *secret, int secret_len, uint8_t *challenge, int challenge_len, uint8_t *digest)
{
	struct sha1_ctx ctx;

	sha1_init(&ctx);
	sha1_update(&ctx, (unsigned char*)&chap_id, 1);
	sha1_update(&ctx, (unsigned char*)secret, secret_len);
	sha1_update(&ctx, challenge, challenge_len);
	sha1_final(&ctx, digest);
}

static int chap_initiator_auth_create_challenge(struct iscsi_connection *conn)
{
	char *value, *p;
	char text[CHAP_CHALLENGE_MAX * 2 + 8];
	static int chap_id;
	int i;

	value = text_key_find(conn, "CHAP_A");
	if (!value)
		return CHAP_INITIATOR_ERROR;
	while ((p = strsep(&value, ","))) {
		if (!strcmp(p, "5")) {
			conn->auth.chap.digest_alg = CHAP_DIGEST_ALG_MD5;
			conn->auth_state = CHAP_AUTH_STATE_CHALLENGE;
			break;
		} else if (!strcmp(p, "7")) {
			conn->auth.chap.digest_alg = CHAP_DIGEST_ALG_SHA1;
			conn->auth_state = CHAP_AUTH_STATE_CHALLENGE;
			break;
		}
	}
	if (!p)
		return CHAP_INITIATOR_ERROR;

	text_key_add(conn, "CHAP_A", p);
	conn->auth.chap.id = ++chap_id;
	sprintf(text, "%u", (unsigned char)conn->auth.chap.id);
	text_key_add(conn, "CHAP_I", text);

	/*
	 * FIXME: does a random challenge length provide any benefits security-
	 * wise, or should we rather always use the max. allowed length of
	 * 1024 for the (unencoded) challenge?
	 */
	conn->auth.chap.challenge_size = (rand() % (CHAP_CHALLENGE_MAX / 2)) + CHAP_CHALLENGE_MAX / 2;

	conn->auth.chap.challenge = malloc(conn->auth.chap.challenge_size);
	if (!conn->auth.chap.challenge)
		return CHAP_TARGET_ERROR;

	p = text;
	strcpy(p, "0x");
	p += 2;
	for (i = 0; i < conn->auth.chap.challenge_size; i++) {
		conn->auth.chap.challenge[i] = rand();
		sprintf(p, "%.2hhx", conn->auth.chap.challenge[i]);
		p += 2;
	}
	text_key_add(conn, "CHAP_C",  text);

	return 0;
}

static int chap_initiator_auth_check_response(struct iscsi_connection *conn)
{
	char *value;
	uint8_t *his_digest = NULL, *our_digest = NULL;
	int digest_len = 0, retval = 0, encoding_format, err;
	char pass[ISCSI_NAME_LEN];

	memset(pass, 0, sizeof(pass));

	err = account_available(conn->tid, AUTH_DIR_INCOMING);
	if (!err) {
		eprintf("No CHAP credentials configured\n");
		retval = CHAP_TARGET_ERROR;
		goto out;
	}

	if (!(value = text_key_find(conn, "CHAP_N"))) {
		retval = CHAP_INITIATOR_ERROR;
		goto out;
	}

	memset(pass, 0, sizeof(pass));
	err = account_lookup(conn->tid, AUTH_DIR_INCOMING, value, 0, pass,
			     ISCSI_NAME_LEN);
	if (err) {
		eprintf("No valid user/pass combination for initiator %s "
			    "found\n", conn->initiator);
		retval = CHAP_AUTH_ERROR;
		goto out;
	}

	if (!(value = text_key_find(conn, "CHAP_R"))) {
		retval = CHAP_INITIATOR_ERROR;
		goto out;
	}

	if ((encoding_format = chap_check_encoding_format(value)) < 0) {
		retval = CHAP_INITIATOR_ERROR;
		goto out;
	}

	switch (conn->auth.chap.digest_alg) {
	case CHAP_DIGEST_ALG_MD5:
		digest_len = CHAP_MD5_DIGEST_LEN;
		break;
	case CHAP_DIGEST_ALG_SHA1:
		digest_len = CHAP_SHA1_DIGEST_LEN;
		break;
	default:
		retval = CHAP_TARGET_ERROR;
		goto out;
	}

	if (!(his_digest = malloc(digest_len))) {
		retval = CHAP_TARGET_ERROR;
		goto out;
	}
	if (!(our_digest = malloc(digest_len))) {
		retval = CHAP_TARGET_ERROR;
		goto out;
	}

	if (chap_decode_string(value, his_digest, digest_len, encoding_format) < 0) {
		retval = CHAP_INITIATOR_ERROR;
		goto out;
	}

	switch (conn->auth.chap.digest_alg) {
	case CHAP_DIGEST_ALG_MD5:
		chap_calc_digest_md5(conn->auth.chap.id, pass, strlen(pass),
				     conn->auth.chap.challenge,
				     conn->auth.chap.challenge_size,
				     our_digest);
		break;
	case CHAP_DIGEST_ALG_SHA1:
		chap_calc_digest_sha1(conn->auth.chap.id, pass, strlen(pass),
				      conn->auth.chap.challenge,
				      conn->auth.chap.challenge_size,
				      our_digest);
		break;
	default:
		retval = CHAP_TARGET_ERROR;
		goto out;
	}

	if (memcmp(our_digest, his_digest, digest_len)) {
		log_warning("CHAP initiator auth.: "
			    "authentication of %s failed (wrong secret!?)",
			    conn->initiator);
		retval = CHAP_AUTH_ERROR;
		goto out;
	}

	conn->state = CHAP_AUTH_STATE_RESPONSE;
 out:
	if (his_digest)
		free(his_digest);
	if (our_digest)
		free(our_digest);
	return retval;
}

static int chap_target_auth_create_response(struct iscsi_connection *conn)
{
	char chap_id, *value, *response = NULL;
	uint8_t *challenge = NULL, *digest = NULL;
	int encoding_format, response_len;
	int challenge_len = 0, digest_len = 0, retval = 0, err;
	char pass[ISCSI_NAME_LEN], name[ISCSI_NAME_LEN];

	if (!(value = text_key_find(conn, "CHAP_I"))) {
		/* initiator doesn't want target auth!? */
		conn->state = STATE_SECURITY_DONE;
		retval = 0;
		goto out;
	}
	chap_id = strtol(value, &value, 10);

	memset(pass, 0, sizeof(pass));
	memset(name, 0, sizeof(name));
	err = account_lookup(conn->tid, AUTH_DIR_OUTGOING, name, sizeof(name),
			     pass, sizeof(pass));
	if (err) {
		log_warning("CHAP target auth.: "
			    "no outgoing credentials configured%s",
			    conn->tid ? "." : " for discovery.");
		retval = CHAP_AUTH_ERROR;
		goto out;
	}

	if (!(value = text_key_find(conn, "CHAP_C"))) {
		log_warning("CHAP target auth.: "
			    "got no challenge from initiator %s",
			    conn->initiator);
		retval = CHAP_INITIATOR_ERROR;
		goto out;
	}

	if ((encoding_format = chap_check_encoding_format(value)) < 0) {
		retval = CHAP_INITIATOR_ERROR;
		goto out;
	}

	retval = chap_alloc_decode_buffer(value, &challenge, encoding_format);
	if (retval <= 0)
		goto out;
	else if (retval > 1024) {
		log_warning("CHAP target auth.: "
			    "initiator %s sent challenge of invalid length %d",
			    conn->initiator, challenge_len);
		retval = CHAP_INITIATOR_ERROR;
		goto out;
	}

	challenge_len = retval;
	retval = 0;

	switch (conn->auth.chap.digest_alg) {
	case CHAP_DIGEST_ALG_MD5:
		digest_len = CHAP_MD5_DIGEST_LEN;
		break;
	case CHAP_DIGEST_ALG_SHA1:
		digest_len = CHAP_SHA1_DIGEST_LEN;
		break;
	default:
		retval = CHAP_TARGET_ERROR;
		goto out;
	}

	if (encoding_format == HEX_FORMAT)
		response_len = 2 * digest_len;
	else
		response_len = ((digest_len - 1) / 3 + 1) * 4;
	//"0x" / "0b" and "\0":
	response_len += 3;

	if (!(digest = malloc(digest_len))) {
		retval = CHAP_TARGET_ERROR;
		goto out;
	}
	if (!(response = malloc(response_len))) {
		retval = CHAP_TARGET_ERROR;
		goto out;
	}

	if (chap_decode_string(value, challenge, challenge_len, encoding_format) < 0) {
		retval = CHAP_INITIATOR_ERROR;
		goto out;
	}

	/* RFC 3720, 8.2.1: CHAP challenges MUST NOT be reused */
	if (challenge_len == conn->auth.chap.challenge_size) {
		if (!memcmp(challenge, conn->auth.chap.challenge,
			    challenge_len)) {
			//FIXME: RFC 3720 demands to close TCP conn.
			log_warning("CHAP target auth.: "
				    "initiator %s reflected our challenge",
				    conn->initiator);
			retval = CHAP_INITIATOR_ERROR;
			goto out;
		}
	}

	switch (conn->auth.chap.digest_alg) {
	case CHAP_DIGEST_ALG_MD5:
		chap_calc_digest_md5(chap_id, pass, strlen(pass),
				     challenge, challenge_len, digest);
		break;
	case CHAP_DIGEST_ALG_SHA1:
		chap_calc_digest_sha1(chap_id, pass, strlen(pass),
				      challenge, challenge_len, digest);
		break;
	default:
		retval = CHAP_TARGET_ERROR;
		goto out;
	}

	memset(response, 0x0, response_len);
	chap_encode_string(digest, digest_len, response, encoding_format);
	text_key_add(conn, "CHAP_N", name);
	text_key_add(conn, "CHAP_R", response);

	conn->state = STATE_SECURITY_DONE;
 out:
	if (challenge)
		free(challenge);
	if (digest)
		free(digest);
	if (response)
		free(response);
	return retval;
}

int cmnd_exec_auth_chap(struct iscsi_connection *conn)
{
	int res;

	switch(conn->auth_state) {
	case CHAP_AUTH_STATE_START:
		res = chap_initiator_auth_create_challenge(conn);
		break;
	case CHAP_AUTH_STATE_CHALLENGE:
		res = chap_initiator_auth_check_response(conn);
		if (res < 0)
			break;
		/* fall through */
	case CHAP_AUTH_STATE_RESPONSE:
		res = chap_target_auth_create_response(conn);
		break;
	default:
		eprintf("BUG. unknown conn->auth_state %d\n", conn->auth_state);
		res = CHAP_TARGET_ERROR;
	}

	return res;
}
