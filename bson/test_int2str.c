#include <stdio.h>
#include <time.h>
#include <assert.h>
#include <string.h>
#include <limits.h>

static void itoa(long int num, char* str) {
    int index = 0;
    int sign = 1;
    // Convert to unsigned to handle -LLONG_MIN overflow
    unsigned long int absNum;

    // Handle the case of 0
    if (num == 0) {
        str[index++] = '0';
        str[index] = '\0';
        return;
    }

    // Handle negative numbers
    if (num < 0) {
        sign = -1;
        absNum = -num;
    } else {
        absNum = num;
    }

    // Convert the number to string
    int digit;
    while (absNum > 0) {
        digit = absNum % 10;
        str[index++] = digit + '0';  // Convert digit to character
        absNum /= 10;
    }

    // Add minus sign if negative
    if (sign == -1) {
        str[index++] = '-';
    }

    str[index] = '\0';  // Null terminator

    // Reverse the string
    int start = 0;
    int end = index - 1;
    while (start < end) {
        char temp = str[start];
        str[start++] = str[end];
        str[end--] = temp;
    }
}

static void int10_to_str(long int val, char *dst)
{
    char buffer[20];
    char *p;
    long int new_val;
    unsigned long int uval = (unsigned long int) val;

    if (val < 0)
    {
        *dst++ = '-';
        /* Avoid integer overflow in (-val) for LLONG_MIN (BUG#31799). */
        uval = (unsigned long int)0 - uval;
    }

    p = &buffer[sizeof(buffer)-1];
    *p = '\0';
    new_val= (long) (uval / 10);
    *--p = '0'+ (char) (uval - (unsigned long) new_val * 10);
    val = new_val;

    while (val != 0)
    {
        new_val=val/10;
        *--p = '0' + (char) (val-new_val*10);
        val= new_val;
    }
    while ((*dst++ = *p++) != 0) ;
    return;
}

static void INT2STRING(long int i, char* buffer) {
    // Macro had access to allocated array but this function
    // only has the pointer, so we hardcode buffer size
    snprintf(buffer, 21, "%ld", i);
    return;
}

void test(void (*f)(long int val, char* str), char* name) {
    // Time common values
    int reps = 10000;
    float startTime, endTime;
    startTime = (float)clock()/CLOCKS_PER_SEC;
    for (int i = 0; i < reps; i++) {
        for (long num = 0; num < 10000; num++) {
            char str[20];
            f(num, str);
        }
    }
    endTime = (float)clock()/CLOCKS_PER_SEC;
    printf("%s: %f\n", name, endTime - startTime);
}

int main() {
    // Test extreme values
    long int maxNum = (((size_t) - 1)>>1);
    long int minNum = -maxNum-1;
    char str_1[21];
    char str_2[21];
    char str_3[21];
    itoa(maxNum, str_1);
    int10_to_str(maxNum, str_2);
    INT2STRING(maxNum, str_3);
    assert(strcmp(str_1, str_2) == 0);
    assert(strcmp(str_1, str_3) == 0);
    memset(str_1,0,strlen(str_1));
    memset(str_2,0,strlen(str_2));
    memset(str_3,0,strlen(str_3));
    itoa(minNum, str_1);
    int10_to_str(minNum, str_2);
    INT2STRING(minNum, str_3);
    assert(strcmp(str_1, str_2) == 0);
    assert(strcmp(str_1, str_3) == 0);

    // Test common values
    for (long num = 0; num < 10000; num++) {
        char str_1[21];
        char str_2[21];
        char str_3[21];
        itoa(num, str_1);
        int10_to_str(num, str_2);
        INT2STRING(num, str_3);
        assert(strcmp(str_1, str_2) == 0);
        assert(strcmp(str_1, str_3) == 0);
    }

    // Time common values
    test(itoa, "itoa");
    test(int10_to_str, "int10_to_str");
    test(INT2STRING, "INT2STRING");

    return 0;
}
