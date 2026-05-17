#include <string.h>
#include "db2_fdw.h"

/** global variables */

/** external variables */

/** external prototypes */

/** local prototypes */
char* db2CopyText (const char* string, int size, int quote);

/* db2CopyText
 * Returns an allocated string containing a (possibly quoted) copy of "string".
 * If the string starts with "(" and ends with ")", no quoting will take place even if "quote" is true.
 */
char* db2CopyText (const char* string, int size, int quote) {
  int      resultsize = (quote ? size + 2 : size);
  register int i; 
  register int j = -1;
  char*    result;

  db2Entry4("(string: '%s', size: %d, quote: %d)",string,size,quote);
  /* if "string" is parenthized, return a copy */
  if (string[0] == '(' && string[size - 1] == ')') {
    result = db2alloc (size + 1, "result");
    memcpy (result, string, size);
    result[size] = '\0';
    return result;
  }

  if (quote) {
    for (i = 0; i < size; ++i) {
      if (string[i] == '"')
        ++resultsize;
    }
  }

  result = db2alloc (resultsize + 1, "result");
  if (quote)
    result[++j] = '"';
  for (i = 0; i < size; ++i) {
    result[++j] = string[i];
    if (quote && string[i] == '"')
      result[++j] = '"';
  }
  if (quote)
    result[++j] = '"';
  result[j + 1] = '\0';

  db2Exit4(": %s",result);
  return result;
}
