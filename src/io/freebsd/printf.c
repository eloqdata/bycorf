/*-
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Copyright (c) 1986, 1988, 1991, 1993
 *	The Regents of the University of California.  All rights reserved.
 * (c) UNIX System Laboratories, Inc.
 * All or some portions of this file are derived from material licensed
 * to the University of California by American Telephone and Telegraph
 * Co. or Unix System Laboratories, Inc. and are reproduced herein with
 * the permission of UNIX System Laboratories, Inc.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */

// Derived from FreeBSD sys/kern/subr_prf.c at af58d0db156a.
// Only the formatter is retained; output goes through the host callback.
#include "internal.h"
#include <sys/ctype.h>
#include <sys/stddef.h>
#include <sys/stdarg.h>
#define MAXNBUF (sizeof(intmax_t) * 8 + 1)

static char* ksprintn(char* nbuf, uintmax_t num, int base, int* lenp,
                      int upper) {
  char *p, c;

  p = nbuf;
  *p = '\0';
  do {
    c = hex2ascii(num % base);
    *++p = upper ? toupper(c) : c;
  } while (num /= base);
  if (lenp) *lenp = p - nbuf;
  return (p);
}

/*
 * Scaled down version of printf(3).
 *
 * Two additional formats:
 *
 * The format %b is supported to decode error registers.
 * Its usage is:
 *
 *	printf("reg=%b\n", regval, "<base><arg>*");
 *
 * where <base> is the output base expressed as a control character, e.g.
 * \10 gives octal; \20 gives hex.  Each arg is a sequence of characters,
 * the first of which gives the bit number to be inspected (origin 1), and
 * the next characters (up to a control character, i.e. a character <= 32),
 * give the name of the register.  Thus:
 *
 *	kvprintf("reg=%b\n", 3, "\10\2BITTWO\1BITONE");
 *
 * would produce output:
 *
 *	reg=3<BITTWO,BITONE>
 *
 * XXX:  %D  -- Hexdump, takes pointer and separator string:
 *		("%6D", ptr, ":")   -> XX:XX:XX:XX:XX:XX
 *		("%*D", len, ptr, " " -> XX XX XX XX ...
 */
int kvprintf(char const* fmt, void (*func)(int, void*), void* arg, int radix,
             va_list ap) {
#define PCHAR(c)        \
  {                     \
    int cc = (c);       \
    if (func)           \
      (*func)(cc, arg); \
    else                \
      *d++ = cc;        \
    retval++;           \
  }
  char nbuf[MAXNBUF];
  char* d;
  const char *p, *percent, *q;
  u_char* up;
  int ch, n, sign;
  uintmax_t num;
  int base, lflag, qflag, tmp, width, ladjust, sharpflag, dot;
  int cflag, hflag, jflag, tflag, zflag;
  int bconv, dwidth, upper;
  char padc;
  int stop = 0, retval = 0;

  num = 0;
  q = NULL;
  if (!func)
    d = (char*)arg;
  else
    d = NULL;

  if (fmt == NULL) fmt = "(fmt null)\n";

  if (radix < 2 || radix > 36) radix = 10;

  for (;;) {
    padc = ' ';
    width = 0;
    while ((ch = (u_char)*fmt++) != '%' || stop) {
      if (ch == '\0') return (retval);
      PCHAR(ch);
    }
    percent = fmt - 1;
    qflag = 0;
    lflag = 0;
    ladjust = 0;
    sharpflag = 0;
    sign = 0;
    dot = 0;
    bconv = 0;
    dwidth = 0;
    upper = 0;
    cflag = 0;
    hflag = 0;
    jflag = 0;
    tflag = 0;
    zflag = 0;
  reswitch:
    switch (ch = (u_char)*fmt++) {
      case '.':
        dot = 1;
        goto reswitch;
      case '#':
        sharpflag = 1;
        goto reswitch;
      case '+':
        sign = '+';
        goto reswitch;
      case '-':
        ladjust = 1;
        goto reswitch;
      case '%':
        PCHAR(ch);
        break;
      case '*':
        if (!dot) {
          width = va_arg(ap, int);
          if (width < 0) {
            ladjust = !ladjust;
            width = -width;
          }
        } else {
          dwidth = va_arg(ap, int);
        }
        goto reswitch;
      case '0':
        if (!dot) {
          padc = '0';
          goto reswitch;
        }
        /* FALLTHROUGH */
      case '1':
      case '2':
      case '3':
      case '4':
      case '5':
      case '6':
      case '7':
      case '8':
      case '9':
        for (n = 0;; ++fmt) {
          n = n * 10 + ch - '0';
          ch = *fmt;
          if (ch < '0' || ch > '9') break;
        }
        if (dot)
          dwidth = n;
        else
          width = n;
        goto reswitch;
      case 'b':
        ladjust = 1;
        bconv = 1;
        goto handle_nosign;
      case 'c':
        width -= 1;

        if (!ladjust && width > 0)
          while (width--) PCHAR(padc);
        PCHAR(va_arg(ap, int));
        if (ladjust && width > 0)
          while (width--) PCHAR(padc);
        break;
      case 'D':
        up = va_arg(ap, u_char*);
        p = va_arg(ap, char*);
        if (!width) width = 16;
        while (width--) {
          PCHAR(hex2ascii(*up >> 4));
          PCHAR(hex2ascii(*up & 0x0f));
          up++;
          if (width)
            for (q = p; *q; q++) PCHAR(*q);
        }
        break;
      case 'd':
      case 'i':
        base = 10;
        goto handle_sign;
      case 'h':
        if (hflag) {
          hflag = 0;
          cflag = 1;
        } else
          hflag = 1;
        goto reswitch;
      case 'j':
        jflag = 1;
        goto reswitch;
      case 'l':
        if (lflag) {
          lflag = 0;
          qflag = 1;
        } else
          lflag = 1;
        goto reswitch;
      case 'n':
        /*
         * We do not support %n in kernel, but consume the
         * argument.
         */
        if (jflag)
          (void)va_arg(ap, intmax_t*);
        else if (qflag)
          (void)va_arg(ap, quad_t*);
        else if (lflag)
          (void)va_arg(ap, long*);
        else if (zflag)
          (void)va_arg(ap, size_t*);
        else if (hflag)
          (void)va_arg(ap, short*);
        else if (cflag)
          (void)va_arg(ap, char*);
        else
          (void)va_arg(ap, int*);
        break;
      case 'o':
        base = 8;
        goto handle_nosign;
      case 'p':
        base = 16;
        sharpflag = (width == 0);
        sign = 0;
        num = (uintptr_t)va_arg(ap, void*);
        goto number;
      case 'q':
        qflag = 1;
        goto reswitch;
      case 'r':
        base = radix;
        if (sign) {
          sign = 0;
          goto handle_sign;
        }
        goto handle_nosign;
      case 's':
        p = va_arg(ap, char*);
        if (p == NULL) p = "(null)";
        if (!dot)
          n = strlen(p);
        else
          for (n = 0; n < dwidth && p[n]; n++) continue;

        width -= n;

        if (!ladjust && width > 0)
          while (width--) PCHAR(padc);
        while (n--) PCHAR(*p++);
        if (ladjust && width > 0)
          while (width--) PCHAR(padc);
        break;
      case 't':
        tflag = 1;
        goto reswitch;
      case 'u':
        base = 10;
        goto handle_nosign;
      case 'X':
        upper = 1;
        /* FALLTHROUGH */
      case 'x':
        base = 16;
        goto handle_nosign;
      case 'y':
        base = 16;
        goto handle_sign;
      case 'z':
        zflag = 1;
        goto reswitch;
      handle_nosign:
        if (jflag)
          num = va_arg(ap, uintmax_t);
        else if (qflag)
          num = va_arg(ap, u_quad_t);
        else if (tflag)
          num = va_arg(ap, ptrdiff_t);
        else if (lflag)
          num = va_arg(ap, u_long);
        else if (zflag)
          num = va_arg(ap, size_t);
        else if (hflag)
          num = (u_short)va_arg(ap, int);
        else if (cflag)
          num = (u_char)va_arg(ap, int);
        else
          num = va_arg(ap, u_int);
        if (bconv) {
          q = va_arg(ap, char*);
          base = *q++;
        }
        goto number;
      handle_sign:
        if (jflag)
          num = va_arg(ap, intmax_t);
        else if (qflag)
          num = va_arg(ap, quad_t);
        else if (tflag)
          num = va_arg(ap, ptrdiff_t);
        else if (lflag)
          num = va_arg(ap, long);
        else if (zflag)
          num = va_arg(ap, ssize_t);
        else if (hflag)
          num = (short)va_arg(ap, int);
        else if (cflag)
          num = (signed char)va_arg(ap, int);
        else
          num = va_arg(ap, int);
        if ((intmax_t)num < 0) {
          sign = '-';
          num = -(intmax_t)num;
        }
      number:
        p = ksprintn(nbuf, num, base, &n, upper);
        tmp = 0;
        if (sharpflag && num != 0) {
          if (base == 8)
            tmp++;
          else if (base == 16)
            tmp += 2;
        }
        if (sign) tmp++;

        if (!ladjust && padc == '0') dwidth = width - tmp;
        width -= tmp + imax(dwidth, n);
        dwidth -= n;
        if (!ladjust)
          while (width-- > 0) PCHAR(' ');
        if (sign) PCHAR(sign);
        if (sharpflag && num != 0) {
          if (base == 8) {
            PCHAR('0');
          } else if (base == 16) {
            PCHAR('0');
            PCHAR('x');
          }
        }
        while (dwidth-- > 0) PCHAR('0');

        while (*p) PCHAR(*p--);

        if (bconv && num != 0) {
          /* %b conversion flag format. */
          tmp = retval;
          while (*q) {
            n = *q++;
            if (num & (1 << (n - 1))) {
              PCHAR(retval != tmp ? ',' : '<');
              for (; (n = *q) > ' '; ++q) PCHAR(n);
            } else
              for (; *q > ' '; ++q) continue;
          }
          if (retval != tmp) {
            PCHAR('>');
            width -= retval - tmp;
          }
        }

        if (ladjust)
          while (width-- > 0) PCHAR(' ');

        break;
      default:
        while (percent < fmt) PCHAR(*percent++);
        /*
         * Since we ignore a formatting argument it is no
         * longer safe to obey the remaining formatting
         * arguments as the arguments will no longer match
         * the format specs.
         */
        stop = 1;
        break;
    }
  }
#undef PCHAR
}

struct celer_format_buffer {
  char* next;
  size_t remaining;
};
static void celer_format_char(int value, void* context) {
  struct celer_format_buffer* b = context;
  if (b->remaining > 1) {
    *b->next++ = value;
    --b->remaining;
  }
}
int vsnprintf(char* str, size_t size, const char* format, va_list args) {
  struct celer_format_buffer b = {str, size};
  int count = kvprintf(format, celer_format_char, &b, 10, args);
  if (size) *b.next = 0;
  return count;
}
int snprintf(char* str, size_t size, const char* format, ...) {
  va_list args;
  va_start(args, format);
  int count = vsnprintf(str, size, format, args);
  va_end(args);
  return count;
}
int sprintf(char* str, const char* format, ...) {
  va_list args;
  va_start(args, format);
  int count = vsnprintf(str, SIZE_MAX, format, args);
  va_end(args);
  return count;
}
int printf(const char* format, ...) {
  char buffer[2048];
  va_list args;
  va_start(args, format);
  int count = vsnprintf(buffer, sizeof(buffer), format, args);
  va_end(args);
  celer_bsd_host.log(buffer, strnlen(buffer, sizeof(buffer)));
  return count;
}
void vlog(int level, const char* format, va_list args) {
  (void)level;
  char buffer[2048];
  vsnprintf(buffer, sizeof(buffer), format, args);
  celer_bsd_host.log(buffer, strnlen(buffer, sizeof(buffer)));
}
void log(int level, const char* format, ...) {
  va_list args;
  va_start(args, format);
  vlog(level, format, args);
  va_end(args);
}
void panic(const char* format, ...) {
  va_list args;
  va_start(args, format);
  vlog(0, format, args);
  va_end(args);
  celer_bsd_host.log("\n", 1);
  celer_bsd_host.abort_process();
  __builtin_trap();
}
