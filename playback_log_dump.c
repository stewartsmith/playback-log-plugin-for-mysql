/* Copyright (c) 2013 Percona Ireland Ltd.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */

/*
  Written by Stewart Smith.

  This program will dump out the Playback log in various formats.

*/

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <endian.h>
#include <stdio.h>
#include <stdlib.h>

#define PLAYBACK_LOG_HEADER_LENGTH  (1 + (6*4) + (2*8))

struct playback_log_entry
{
  uint64_t rows_sent;
  char* query;
};

static int parse_playback_log_header(int fd, char *header)
{
  if(header[0] != 1) {
    fprintf(stderr, "ERROR: Invalid playback log entry type %d\n", header[0]);
    return -1;
  }

  uint64_t general_rows= le64toh(*(uint32_t*)(header+1+(6*4)+8)); // general_rows

  uint32_t thread_id= le32toh(*(uint32_t*)(header+1+2*4));
  ssize_t user_length= (ssize_t)le32toh(*(uint32_t*)(header+1+3*4));
  ssize_t command_length= (ssize_t)le32toh(*(uint32_t*)(header+1+4*4));
  ssize_t query_length= (ssize_t)le32toh(*(uint32_t*)(header+1+5*4));

  fprintf(stdout, "/* Thread Id: %d Rows: %ld ",
	  thread_id, general_rows);

  char static_buf[1024];
  char *buf;

  ssize_t maxlen= (user_length > command_length)? user_length : command_length;
  if (query_length > maxlen)
    maxlen= query_length;

  if (maxlen < 1024)
    buf= static_buf;
  else
    buf= (char*) malloc(query_length);

  ssize_t r;
  r= read(fd, buf, user_length);

  if(r != user_length) {
    fprintf(stderr, "ERROR: Couldn't read all of user\n");
    return -2;
  }

  fprintf(stdout, "User: ");
  fwrite(buf, user_length, 1, stdout);

  r= read(fd, buf, command_length);

  if(r != command_length) {
    fprintf(stderr, "ERROR: Couldn't read all of command\n");
    return -2;
  }

  fprintf(stdout, " Command: ");
  fwrite(buf, command_length, 1, stdout);

  r= read(fd, buf, query_length);

  if(r != query_length) {
    fprintf(stderr, "ERROR: Couldn't read all of query\n");
    return -2;
  }

  /* FIXME: handle nulls in stream */
  fprintf(stdout, " */ ");
  fwrite(buf, query_length, 1, stdout);
  fprintf(stdout, ";\n");

  if (static_buf != buf)
    free(buf);

  return 0;
}


int main(int argc, char* argv[])
{
  int opt;
  char *filename= NULL;
  int fd;

  while ((opt= getopt(argc, argv, "f:")) != -1) {
    switch (opt) {
    case 'f':
      filename= optarg;
      break;
    default:
      break;
    }
  }

  if (filename)
    fd= open(filename, O_RDONLY);
  else
    fd= fileno(stdin);

  if (fd == -1) {
    fprintf(stderr, "could not open %s for reading\n", (filename)? filename : "stdin");
    exit(1);
  }

  do {
    char header[PLAYBACK_LOG_HEADER_LENGTH];

    ssize_t header_sz = read(fd, header, PLAYBACK_LOG_HEADER_LENGTH);

    if (header_sz == 0)
      break;

    if (header_sz != PLAYBACK_LOG_HEADER_LENGTH) {
      fprintf(stderr, "ERROR: playback log header length mismatch %d !+ %d\n",
	      (int)header_sz, PLAYBACK_LOG_HEADER_LENGTH);
      break;
    }

    int r= parse_playback_log_header(fd, header);

    if (r!=0)
      break;

  } while(1);

  exit(0);
}
