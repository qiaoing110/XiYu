package com.keyscafe.unlock

import io.github.libxposed.api.XposedModule

internal fun cnWalletSpoof(key: String?): String? = when (key) {
    "ro.csc.sales_code", "persist.omc.sales_code", "ril.sales_code" -> "CHC"
    "ro.csc.countryiso_code" -> "CN"
    "ro.csc.country_code" -> "China"
    else -> null
}

object WalletHooks {

    val TARGET_PACKAGES = setOf(
        "com.samsung.android.spay",
        "com.samsung.android.spayfw",
        "com.unionpay.tsmservice"
    )

    private val CONST_SPOOF = mapOf(
        "getSalesCode" to "CHC",
        "getCountryIso" to "CN",
        "getCountryCode" to "China"
    )

    @Volatile
    private var installed = false

    fun install(module: XposedModule, classLoader: ClassLoader) {
        if (installed) return
        val enabled = runCatching {
            module.getRemotePreferences(Prefs.GROUP).getBoolean(Prefs.CN_WALLET, true)
        }.getOrDefault(true)
        if (!enabled) return
        installed = true

        hookSystemProperties(module, classLoader)
        hookSemProperties(module, classLoader)
        XLog.i("WalletHooks: 就绪")
    }

    private fun hookSystemProperties(module: XposedModule, classLoader: ClassLoader) {
        val cls = Reflect.findClassIfExists("android.os.SystemProperties", classLoader) ?: return
        cls.declaredMethods
            .filter { it.name in setOf("get", "native_get") && it.parameterTypes.firstOrNull() == String::class.java }
            .forEach { m ->
                XLog.safe("WalletHooks.SystemProperties.${m.name}") {
                    module.hook(m).intercept { chain ->
                        cnWalletSpoof(chain.getArg(0) as? String) ?: chain.proceed()
                    }
                }
            }
    }

    private fun hookSemProperties(module: XposedModule, classLoader: ClassLoader) {
        val cls = Reflect.findClassIfExists("android.os.SemSystemProperties", classLoader) ?: return
        cls.declaredMethods.forEach { m ->
            when {
                m.name == "get" && m.parameterTypes.firstOrNull() == String::class.java -> {
                    XLog.safe("WalletHooks.SemSystemProperties.get") {
                        module.hook(m).intercept { chain ->
                            cnWalletSpoof(chain.getArg(0) as? String) ?: chain.proceed()
                        }
                    }
                }
                m.name in CONST_SPOOF && m.parameterCount == 0 -> {
                    val value = CONST_SPOOF.getValue(m.name)
                    XLog.safe("WalletHooks.SemSystemProperties.${m.name}") {
                        module.hook(m).intercept { value }
                    }
                }
            }
        }
    }
}
