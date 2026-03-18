package net.ninjacat.rowcp.data

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class TableSkipFilterTest {

    @Test
    fun `should not skip table not in list`() {
        val filter = TableSkipFilter(listOf("orders", "payments"))
        assertThat(filter.shouldSkip("users")).isFalse()
    }

    @Test
    fun `should skip table that is in list`() {
        val filter = TableSkipFilter(listOf("orders", "payments"))
        assertThat(filter.shouldSkip("orders")).isTrue()
    }

    @Test
    fun `should skip table matching regex pattern`() {
        val filter = TableSkipFilter(listOf("tmp_.*"))
        assertThat(filter.shouldSkip("tmp_cache")).isTrue()
    }

    @Test
    fun `should not skip table not matching regex pattern`() {
        val filter = TableSkipFilter(listOf("tmp_.*"))
        assertThat(filter.shouldSkip("cache")).isFalse()
    }

    @Test
    fun `should not skip anything when list is empty`() {
        val filter = TableSkipFilter(emptyList())
        assertThat(filter.shouldSkip("any_table")).isFalse()
    }

    @Test
    fun `should skip table matching any of multiple patterns`() {
        val filter = TableSkipFilter(listOf("tmp_.*", "audit_.*"))
        assertThat(filter.shouldSkip("audit_log")).isTrue()
        assertThat(filter.shouldSkip("tmp_data")).isTrue()
        assertThat(filter.shouldSkip("users")).isFalse()
    }
}

internal class TableUseFilterTest {

    @Test
    fun `should not skip table that is in the allowlist`() {
        val filter = TableUseFilter(listOf("orders", "payments"))
        assertThat(filter.shouldSkip("orders")).isFalse()
    }

    @Test
    fun `should skip table not in the allowlist`() {
        val filter = TableUseFilter(listOf("orders", "payments"))
        assertThat(filter.shouldSkip("users")).isTrue()
    }

    @Test
    fun `should not skip table matching regex pattern`() {
        val filter = TableUseFilter(listOf("order_.*"))
        assertThat(filter.shouldSkip("order_items")).isFalse()
    }

    @Test
    fun `should skip table not matching regex pattern`() {
        val filter = TableUseFilter(listOf("order_.*"))
        assertThat(filter.shouldSkip("users")).isTrue()
    }

    @Test
    fun `should not skip table matching any of multiple patterns`() {
        val filter = TableUseFilter(listOf("orders", "payment_.*"))
        assertThat(filter.shouldSkip("orders")).isFalse()
        assertThat(filter.shouldSkip("payment_log")).isFalse()
    }

    @Test
    fun `should skip table not matching any of multiple patterns`() {
        val filter = TableUseFilter(listOf("orders", "payment_.*"))
        assertThat(filter.shouldSkip("users")).isTrue()
    }
}
