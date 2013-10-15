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

  This plugin will produce a log suitable for feeding into Percona Playback

  The aim is to have near zero impact on running database server (except for
  extra IO)
*/

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <endian.h>

#include <my_sys.h>
#include <mysql/plugin.h>
#include <mysql/plugin_audit.h>

static my_bool playback_log_enabled = TRUE;
static char *playback_log_filename= NULL;

static int fd= -1;

static int playback_log_plugin_init(void *arg __attribute__((unused)))
{
  fd= open(playback_log_filename, O_WRONLY|O_APPEND|O_CREAT, 0600);

  if (fd == -1)
    return -1;

  return(0);
}

static int playback_log_plugin_deinit(void *arg __attribute__((unused)))
{
  close(fd);
  return(0);
}

static size_t event_general_size(struct mysql_event_general *e)
{
  /*
    File format is little endian.

    byte     type; (1 = general event)
    uint32_t event_subclass;
    uint32_t general_error_code;
    uint32_t general_thread_id;
    uint32_t general_user_length;
    uint32_t general_command_length;
    uint32_t general_query_length;
    uint64_t general_time;
    uint64_t general_rows;

    the following are sequential, each of their respected length of bytes:
    general_user;
    general_command;
    general_query;
  */
  return 1 + (6*4) + (2*8)
    + e->general_user_length
    + e->general_command_length
    + e->general_query_length;
}

static void format_event_general(char *buf, struct mysql_event_general *e)
{
  *(buf++)= 1; // event type;
  *((uint32_t*)buf) = htole32(e->event_subclass);
  buf+=sizeof(uint32_t);
  *((uint32_t*)buf) = htole32(e->general_error_code);
  buf+=sizeof(uint32_t);
  *((uint32_t*)buf) = htole32(e->general_thread_id);
  buf+=sizeof(uint32_t);
  *((uint32_t*)buf) = htole32(e->general_user_length);
  buf+=sizeof(uint32_t);
  *((uint32_t*)buf) = htole32(e->general_command_length);
  buf+=sizeof(uint32_t);
  *((uint32_t*)buf) = htole32(e->general_query_length);
  buf+=sizeof(uint32_t);

  *((uint64_t*)buf) = htole64(e->general_time);
  buf+=sizeof(uint64_t);
  *((uint64_t*)buf) = htole64(e->general_rows);
  buf+=sizeof(uint64_t);

  memcpy(buf, e->general_user, e->general_user_length);
  buf+= e->general_user_length;
  memcpy(buf, e->general_command, e->general_command_length);
  buf+= e->general_command_length;
  memcpy(buf, e->general_query, e->general_query_length);
}


static void playback_log_notify(MYSQL_THD thd __attribute__((unused)),
                              unsigned int event_class,
                              const void *event)
{
  char static_buf[1024];

  if (event_class == MYSQL_AUDIT_GENERAL_CLASS)
  {
    const struct mysql_event_general *event_general=
      (const struct mysql_event_general *) event;

    char *buf;
    size_t s= event_general_size(event_general);

    if(s <= 1024)
      buf= static_buf;
    else
      buf= (char*)malloc(s);

    format_event_general(buf, event_general);

    write(fd, buf, s);

    if (static_buf != buf)
      free(buf);
  }
}


static struct st_mysql_audit playback_log_descriptor=
{
  MYSQL_AUDIT_INTERFACE_VERSION,
  NULL,
  playback_log_notify,
  { (unsigned long) MYSQL_AUDIT_CONNECTION_CLASSMASK |
    MYSQL_AUDIT_GENERAL_CLASSMASK }
};


static int filename_validate(MYSQL_THD* thd,
                             struct st_mysql_sys_var* var,
                             void* save,
                             struct st_mysql_value* value)
{
  int new_fd= -1;

  new_fd= open(playback_log_filename, O_WRONLY|O_APPEND|O_CREAT, 0600);

  if (new_fd == -1)
    return 0;

  if (fd != -1)
    close(fd);

  fd= new_fd;

  return 1;
}

static void filename_update(MYSQL_THD* thd,
                            struct st_mysql_sys_var* var,
                            void* var_ptr,
                            const void* save)
{
}

static void playback_log_enabled_set(MYSQL_THD* thd,
                                          struct st_mysql_sys_var* var,
                                          void* var_ptr,
                                          const void* save)
{
  my_bool new_value= playback_log_enabled;
  my_bool old_value= *(my_bool*) save;

//  ensure_log_file_open();

  playback_log_enabled= *(my_bool*) save;

  if (*(my_bool*) save)
  {
    // TODO: disable log
  }
  else
  {
    // TODO: enable log
  }

}

static MYSQL_SYSVAR_BOOL(enable, playback_log_enabled,
  PLUGIN_VAR_OPCMDARG,
  "Enable playback log.",
  NULL, playback_log_enabled_set, FALSE);

static MYSQL_SYSVAR_STR(filename, playback_log_filename,
  PLUGIN_VAR_RQCMDARG,
  "Filename for playback log.",
  filename_validate,
  filename_update, "playback.log");

static struct st_mysql_sys_var* system_variables[]= {
  MYSQL_SYSVAR(enable),
  MYSQL_SYSVAR(filename),
  NULL
};

mysql_declare_plugin(playback_log)
{
  MYSQL_AUDIT_PLUGIN,
  &playback_log_descriptor,
  "playback_log",
  "Stewart Smith, Percona",
  "Creates compact query logs suitable for feeding to Percona Playback",
  PLUGIN_LICENSE_GPL,
  playback_log_plugin_init,
  playback_log_plugin_deinit,
  0x0001,
  NULL,
  system_variables,
  NULL,
  0,
}
mysql_declare_plugin_end;

