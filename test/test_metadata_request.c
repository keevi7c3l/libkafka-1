#include <stdio.h>
#include <kafka.h>

static uint8_t reference[] = {
	0x00, 0x00, 0x00, 0x16, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
	0x08, 0x6C, 0x69, 0x62, 0x6B, 0x61, 0x66, 0x6B, 0x61, 0x00, 0x00, 0x00, 0x00
};

static void print_bytes(uint8_t *ptr, size_t len)
{
	size_t i;
	for (i = 0; i < len; i++)
		printf("0x%02X ", ptr[i]);
	printf("\n");
}

int main(int argc, char **argv)
{
	struct metadata_request *r;
	uint8_t *buf;
	size_t len;
	r = metadata_request_new(NULL, "libkafka");
	if (!r)
		return -1;
	len = metadata_request_to_buffer(r, &buf);
	if (memcmp(buf, reference, len) == 0)
		return 0;
	return -1;
}
