// AAC-ELD encoder for HomeKit talk-back: reads s16le 16kHz mono PCM from stdin,
// emits one raw access unit per frame to stdout as [uint16 BE length][AU bytes].
// Frame length is forced to 480 samples to match what HomeKit accessories expect.
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fdk-aac/aacenc_lib.h>

static void die(const char *m) { fprintf(stderr, "eld_encoder: %s\n", m); exit(1); }

int main(void) {
    HANDLE_AACENCODER enc;
    if (aacEncOpen(&enc, 0, 1) != AACENC_OK) die("open");
    if (aacEncoder_SetParam(enc, AACENC_AOT, 39) != AACENC_OK) die("aot");          // ELD
    if (aacEncoder_SetParam(enc, AACENC_SAMPLERATE, 16000) != AACENC_OK) die("rate");
    if (aacEncoder_SetParam(enc, AACENC_CHANNELMODE, MODE_1) != AACENC_OK) die("chan");
    if (aacEncoder_SetParam(enc, AACENC_BITRATE, 24000) != AACENC_OK) die("bitrate");
    if (aacEncoder_SetParam(enc, AACENC_TRANSMUX, 0) != AACENC_OK) die("transmux");  // raw AU
    if (aacEncoder_SetParam(enc, AACENC_GRANULE_LENGTH, 480) != AACENC_OK) die("granule");
    if (aacEncoder_SetParam(enc, AACENC_SBR_MODE, 0) != AACENC_OK) die("sbr");
    if (aacEncoder_SetParam(enc, AACENC_AFTERBURNER, 1) != AACENC_OK) die("afterburner");
    AACENC_ERROR _ie = aacEncEncode(enc, NULL, NULL, NULL, NULL);
    if (_ie != AACENC_OK) { fprintf(stderr, "init err 0x%04x\n", _ie); return 1; }

    AACENC_InfoStruct info;
    if (aacEncInfo(enc, &info) != AACENC_OK) die("info");
    // Announce the AudioSpecificConfig + frame length on stderr for the caller.
    fprintf(stderr, "frameLength=%u confSize=%u conf=", info.frameLength, info.confSize);
    for (unsigned i = 0; i < info.confSize; i++) fprintf(stderr, "%02x", info.confBuf[i]);
    fprintf(stderr, "\n"); fflush(stderr);

    const int N = 480;
    short pcm[480];
    unsigned char out[4096];

    for (;;) {
        size_t got = fread(pcm, sizeof(short), N, stdin);
        if (got == 0) break;
        if (got < (size_t)N) memset(pcm + got, 0, (N - got) * sizeof(short));

        void *inbuf = pcm; INT inid = IN_AUDIO_DATA; INT insz = N * (INT)sizeof(short); INT inel = sizeof(short);
        void *obuf = out;  INT oid = OUT_BITSTREAM_DATA; INT osz = sizeof(out); INT oel = 1;
        AACENC_BufDesc ind = {1, &inbuf, &inid, &insz, &inel};
        AACENC_BufDesc outd = {1, &obuf, &oid, &osz, &oel};
        AACENC_InArgs ia = {0}; ia.numInSamples = N;
        AACENC_OutArgs oa = {0};
        AACENC_ERROR e = aacEncEncode(enc, &ind, &outd, &ia, &oa);
        if (e != AACENC_OK) die("encode");
        if (oa.numOutBytes > 0) {
            unsigned char hdr[2] = { (unsigned char)(oa.numOutBytes >> 8), (unsigned char)(oa.numOutBytes & 0xff) };
            fwrite(hdr, 1, 2, stdout);
            fwrite(out, 1, oa.numOutBytes, stdout);
            fflush(stdout);
        }
    }
    aacEncClose(&enc);
    return 0;
}
