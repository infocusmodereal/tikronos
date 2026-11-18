package tikronos

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import tikronos.codec.parseValue
import tikronos.model.Value

class BinaryNormalizationTest {
    @Test
    fun `binary values keep canonical padding and reject malformed inputs`() {
        for ((input, canonical) in listOf("AQ==" to "AQ==", "AR==" to "AQ==", "AQ" to "AQ==",
            "AQI=" to "AQI=", "AQJ=" to "AQI=", "AQID" to "AQID", "" to "")) {
            assertEquals(canonical, (parseValue(obj("B" to input)) as Value.Binary).base64)
        }
        assertFailsWith<ApiError> { parseValue(obj("B" to "AQ=")) }
    }
}
