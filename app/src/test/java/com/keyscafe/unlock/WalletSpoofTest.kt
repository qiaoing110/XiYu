package com.keyscafe.unlock

import org.junit.Assert.assertEquals
import org.junit.Assert.assertNull
import org.junit.Test

class WalletSpoofTest {

    @Test
    fun salesCodeKeysMapToChc() {
        assertEquals("CHC", cnWalletSpoof("ro.csc.sales_code"))
        assertEquals("CHC", cnWalletSpoof("persist.omc.sales_code"))
        assertEquals("CHC", cnWalletSpoof("ril.sales_code"))
    }

    @Test
    fun countryKeysMapToChina() {
        assertEquals("CN", cnWalletSpoof("ro.csc.countryiso_code"))
        assertEquals("China", cnWalletSpoof("ro.csc.country_code"))
    }

    @Test
    fun otherKeysUntouched() {
        // 未命中保持原值，避免误伤 carrierid 等其它属性
        assertNull(cnWalletSpoof("ro.boot.carrierid"))
        assertNull(cnWalletSpoof("ro.product.model"))
        assertNull(cnWalletSpoof(""))
        assertNull(cnWalletSpoof(null))
    }
}
