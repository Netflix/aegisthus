import groovy.json.JsonSlurper
import spock.lang.Specification

import org.apache.hadoop.util.ProgramDriver
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import com.google.common.base.Splitter
import com.google.common.io.Files
import com.netflix.Aegisthus

class AegisthusIntegrationSpec extends Specification {
    private static final Logger LOG = LoggerFactory.getLogger(AegisthusIntegrationSpec)
    private static final TAB_SPLITTER = Splitter.on('\t').limit(2)

    private void checkFile(File file) {
        assert file, 'Unable to check null file'
        assert file.exists(), "File $file does not exist"
        assert file.canRead(), "Cannot read from file $file"
    }

    private void checkDirectory(File directory) {
        checkFile(directory)
        assert directory.isDirectory(), "$directory is not a directory"
    }

    private File getResourceDirectory(String resource) {
        URL url = AegisthusIntegrationSpec.getResource(resource)
        assert url, "Unable to locate resource $resource"
        def file = new File(url.toURI())
        checkDirectory(file)
        return file
    }

    private Map convertAegJsonOutputToJson(File input) {
        checkFile(input)
        def output = [:]

        input.eachWithIndex { String line, int index ->
            List<String> parts = TAB_SPLITTER.split(line).toList()
            String key = parts.first()
            String jsonText = parts.last()

            def parser = new JsonSlurper()
            def jsonObject = parser.parseText(jsonText)
            output[key] = jsonObject[key]
        }

        return output
    }

    private void compareAegJsonMaps(Map expected, Map actual, List<String> rowsToSkip = []) {
        Set<String> expectedKeys = expected.keySet()
        Set<String> actualKeys = actual.keySet()

        if (expectedKeys != actualKeys) {
            if (expectedKeys.size() < actualKeys.size()) {
                def difference = actualKeys - expectedKeys
                throw new AssertionError("Expect the keys to be the same but there were too many keys.  These are the additonal keys: ${difference}")
            } else {
                def difference = expectedKeys - actualKeys
                throw new AssertionError("Expect the keys to be the same but there were too few keys.  These are the missing keys: ${difference}")
            }
        }

        expectedKeys.each { String key ->
            Map expectedValue = expected[key] as Map
            Map actualValue = actual[key] as Map

            if (key in rowsToSkip) {
                LOG.info("SKIPPING results for key $key")
                return
            } else {
                LOG.info("Comparing results for key $key")
            }
            assert expectedValue.deletedAt == actualValue.deletedAt

            List expectedColumns = expectedValue.columns
            List actualColumns = actualValue.columns

            // While running with the non-columnar output the columns were not sorted correctly therefore we are sorting these alphabetically
            expectedColumns.sort(true) { List a, List b ->
                a.first() <=> b.first()
            }
            actualColumns.sort(true) { List a, List b ->
                a.first() <=> b.first()
            }

            for (int i = 0; i < Math.max(expectedColumns.size(), actualColumns.size()); i++) {
                List expectedColumn = expectedColumns.size() > i ? expectedColumns[i] as List : []
                List actualColumn = actualColumns.size() > i ? actualColumns[i] as List : []
                List columnWithData = expectedColumn ?: actualColumn

                LOG.info("Comparing results for key $key column ${columnWithData.first()}")
                assert expectedColumn == actualColumn
            }
        }
    }

    private void compareAegJsonOutput(File expectedOutput, File actualOutput, List<String> rowsToSkip = []) {
        checkFile(expectedOutput)
        checkFile(actualOutput)
        LOG.info("Comparing ${expectedOutput.absolutePath} to ${actualOutput.absolutePath}")
        LOG.error("vimdiff '${expectedOutput.absolutePath}' '${actualOutput.absolutePath}'")

        Map expectedJson = convertAegJsonOutputToJson(expectedOutput)
        Map actualJson = convertAegJsonOutputToJson(actualOutput)
        compareAegJsonMaps(expectedJson, actualJson, rowsToSkip)
    }

    def 'test aegisthus json output on cassandra 1.2.18 input'() {
        setup:
        def outputDir = Files.createTempDir()
        def programDriver = new ProgramDriver()
        programDriver.addClass('aegisthus', Aegisthus, 'aegisthus')
        String[] aegisthusCommandLine = [
                'aegisthus',
                '-inputDir', getResourceDirectory('/testdata/1.2.18/randomtable/input').absolutePath,
                '-output', outputDir.absolutePath
        ]
        File expectedOutput = new File(getResourceDirectory('/testdata/1.2.18/randomtable/aeg_json_output'), 'aeg-00000')

        when: 'when aegisthus is run with a fresh output directory'
        outputDir.delete()
        def exitCode = programDriver.run(aegisthusCommandLine)

        then: 'exitCode should be zero'
        !exitCode

        and: 'and the output should match the expected json output'
        def actualOutput = new File(outputDir, 'aeg-00000')
        // Skip these rows because they are output incorrectly included by the old data output
        def skippedRows = ['0000002d', '0000004b']
        compareAegJsonOutput(expectedOutput, actualOutput, skippedRows)
    }

    def 'test aegisthus columnar output on cassandra 1.2.18 input'() {
        setup:
        def columnarOutputDirectory = Files.createTempDir()
        def jsonOutputDirectory = Files.createTempDir()
        def programDriver = new ProgramDriver()
        programDriver.addClass('aegisthus', Aegisthus, 'aegisthus')
        String[] aegisthusCommandLine = [
                'aegisthus',
                '-inputDir', getResourceDirectory('/testdata/1.2.18/randomtable/input').absolutePath,
                '-output', columnarOutputDirectory.absolutePath,
                '-produceSSTable'
        ]
        File expectedOutput = new File(getResourceDirectory('/testdata/1.2.18/randomtable/aeg_json_output'), 'aeg-00000')

        when: 'when aegisthus is run with a fresh output directory'
        columnarOutputDirectory.delete()
        def exitCode = programDriver.run(aegisthusCommandLine)

        then: 'exitCode should be zero'
        !exitCode

        and: 'the columnar output file should exist'
        def columnarOutput = new File(columnarOutputDirectory, 'keyspace-dataset-ic-0000000000-Data.db')
        checkFile(columnarOutput)

        when: 'aegisthus is run against the just produced columnar output'
        aegisthusCommandLine = [
                'aegisthus',
                '-input', columnarOutput.absolutePath,
                '-output', jsonOutputDirectory.absolutePath
        ]
        jsonOutputDirectory.delete()
        exitCode = programDriver.run(aegisthusCommandLine)

        then: 'exitCode should be zero'
        !exitCode

        and: 'and the output should match the expected json output'
        def actualOutput = new File(jsonOutputDirectory, 'aeg-00000')
        compareAegJsonOutput(expectedOutput, actualOutput)
    }

    def 'test aegisthus columnar rangetombstone on cassandra 1.2.18 input'() {
        setup:
        def columnarOutputDirectory = Files.createTempDir()
        def programDriver = new ProgramDriver()
        programDriver.addClass('aegisthus', Aegisthus, 'aegisthus')
        String[] aegisthusCommandLine = [
                'aegisthus',
                '-inputDir', getResourceDirectory('/testdata/1.2.18/rangetombstone/input').absolutePath,
                '-output', columnarOutputDirectory.absolutePath,
                '-produceSSTable'
        ]
        File expectedOutput = new File(getResourceDirectory('/testdata/1.2.18/rangetombstone/aeg_sstable_output'), 'keyspace-dataset-ic-0000000000-Data.db')
        checkFile(expectedOutput)

        when: 'when aegisthus is run with a fresh output directory'
        columnarOutputDirectory.delete()
        def exitCode = programDriver.run(aegisthusCommandLine)

        then: 'exitCode should be zero'
        !exitCode

        and: 'the columnar output file should exist'
        def columnarOutput = new File(columnarOutputDirectory, 'keyspace-dataset-ic-0000000000-Data.db')
        checkFile(columnarOutput)

        and: 'and the output should match the cassandra compacted output'
        System.err.println "Checking output file ${columnarOutput.absolutePath} against compacted file ${expectedOutput.absolutePath}"
        byte[] expectedOutputBytes = expectedOutput.bytes
        byte[] columnarOutputBytes = columnarOutput.bytes
        expectedOutputBytes.length == columnarOutputBytes.length
        expectedOutputBytes == columnarOutputBytes
    }

    def 'test aegisthus json output on cassandra 1.2.18 input that has rangetombstone'() {
        setup:
        def columnarOutputDirectory = Files.createTempDir()
        def jsonOutputDirectory = Files.createTempDir()
        def programDriver = new ProgramDriver()
        programDriver.addClass('aegisthus', Aegisthus, 'aegisthus')
        String[] aegisthusCommandLine = [
                'aegisthus',
                '-inputDir', getResourceDirectory('/testdata/1.2.18/rangetombstone/input').absolutePath,
                '-output', columnarOutputDirectory.absolutePath,
                '-produceSSTable'
        ]
        String expectedResults = new File(getResourceDirectory('/testdata/1.2.18/rangetombstone/aeg_json_output'), 'aeg-00000').text

        when: 'when aegisthus is run with a fresh output directory'
        columnarOutputDirectory.delete()
        def exitCode = programDriver.run(aegisthusCommandLine)

        then: 'exitCode should be zero'
        !exitCode

        and: 'the columnar output file should exist'
        def columnarOutput = new File(columnarOutputDirectory, 'keyspace-dataset-ic-0000000000-Data.db')
        checkFile(columnarOutput)

        when: 'aegisthus is run against the just produced columnar output'
        aegisthusCommandLine = [
                'aegisthus',
                '-input', columnarOutput.absolutePath,
                '-output', jsonOutputDirectory.absolutePath
        ]
        jsonOutputDirectory.delete()
        exitCode = programDriver.run(aegisthusCommandLine)

        then: 'exitCode should be zero'
        !exitCode

        and: 'and the output should match the expected json output'
        def outputFile = new File(jsonOutputDirectory, 'aeg-00000'/**/)
        checkFile(outputFile)
        List<String> expectedLines = expectedResults.readLines()
        List<String> outputLines = outputFile.readLines()
        expectedLines.size() == outputLines.size()
        for (int i = 0; i < expectedLines.size(); i++) {
            assert expectedLines[i] == outputLines[i], "Mismatch on line ${i + 1}"
        }
    }
}
