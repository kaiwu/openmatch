#include <check.h>
#include <stdlib.h>

// Declare suites from other files
Suite* slab_suite(void);
Suite* orderbook_suite(void);

int main(void) {
    int number_failed;
    SRunner *sr = srunner_create(slab_suite());
    srunner_add_suite(sr, orderbook_suite());
    
    srunner_run_all(sr, CK_NORMAL);
    number_failed = srunner_ntests_failed(sr);
    srunner_free(sr);
    
    return (number_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
