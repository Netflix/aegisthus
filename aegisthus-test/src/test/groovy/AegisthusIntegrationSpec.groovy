import com.google.common.base.Splitter
import com.google.common.io.Files
import com.netflix.Aegisthus
import groovy.json.JsonSlurper
import org.apache.hadoop.util.ProgramDriver
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.IgnoreRest
import spock.lang.Specification
import spock.lang.Unroll

class AegisthusIntegrationSpec extends Specification {
    private static final Logger LOG = LoggerFactory.getLogger(AegisthusIntegrationSpec)
    private static final TAB_SPLITTER = Splitter.on('\t').limit(2)

    private File checkFile(File file) {
        assert file, 'Unable to check null file'
        assert file.exists(), "File $file does not exist"
        assert file.canRead(), "Cannot read from file $file"
        return file
    }

    private File checkDirectory(File directory) {
        checkFile(directory)
        assert directory.isDirectory(), "$directory is not a directory"
        return directory
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

    private void compareAegJsonMaps(Map expected, Map actual) {
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

            LOG.info("Comparing results for key $key")
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

    private void compareAegJsonOutput(File expectedOutput, File actualOutput) {
        checkFile(expectedOutput)
        checkFile(actualOutput)
        LOG.info("(comparing) vimdiff '${expectedOutput.absolutePath}' '${actualOutput.absolutePath}'")

        Map expectedJson = convertAegJsonOutputToJson(expectedOutput)
        Map actualJson = convertAegJsonOutputToJson(actualOutput)
        compareAegJsonMaps(expectedJson, actualJson)
    }

    private File runAegisthusAndReturnOutputFile(Map input) {
        assert (input.inputDirectory && input.inputDirectory instanceof File) || (input.inputFile && input.inputFile instanceof File)
        assert input.outputFileName
        File inputDirectory = input.inputDirectory
        File inputFile = input.inputFile
        assert !inputDirectory || !inputFile

        File outputDir = Files.createTempDir()
        String outdirAbsolutePath = outputDir.absolutePath
        // After storing the name of the directory we delete it so it will not exists when Aegisthus runs
        outputDir.delete()

        def aegisthusCommandLine = ['aegisthus']

        if (input.forceSplittingInput) {
            aegisthusCommandLine << '-D' << "${Aegisthus.Feature.CONF_BLOCKSIZE}=1024"
        }
        aegisthusCommandLine << '-D' << "${Aegisthus.Feature.CONF_MAX_CORRUPT_FILES_TO_SKIP}=1"

        if (input.outputSSTableVersion) {
            aegisthusCommandLine <<
                    "-${Aegisthus.Feature.CMD_ARG_SSTABLE_OUTPUT_VERSION}" <<
                    input.outputSSTableVersion
        }

        if (inputDirectory) {
            aegisthusCommandLine << '-inputDir' << inputDirectory.absolutePath
        } else if (inputFile) {
            aegisthusCommandLine << '-input' << inputFile.absolutePath
        }

        if (input.containsKey('produceSSTable')) {
            aegisthusCommandLine << '-produceSSTable'
        }

        aegisthusCommandLine << '-output' << outdirAbsolutePath

        LOG.info("Running aegisthus: {}", aegisthusCommandLine)

        def programDriver = new ProgramDriver()
        programDriver.addClass('aegisthus', Aegisthus, 'aegisthus')
        def exitCode = programDriver.run(aegisthusCommandLine.toArray() as String[])
        assert exitCode == 0

        File actualOutput = new File(outdirAbsolutePath, input.outputFileName)
        return checkFile(actualOutput)
    }

    @Unroll('Read sstables from cassandra and generate aegisthus json output')
    def 'test sstable to json'() {
        when: 'aegisthus is run'
        def actualOutput = runAegisthusAndReturnOutputFile([
                inputDirectory     : inputDirectory,
                outputFileName     : outputFileName,
                forceSplittingInput: forceSplittingInput
        ])

        then: 'and the output should match the expected json output'
        compareAegJsonOutput(expectedOutput, actualOutput)
        // Sanity check that the files are identical
        expectedOutput.text == actualOutput.text

        where:
        inputDirectory                                                             | outputFileName | expectedOutput                                                                                              | forceSplittingInput
        getResourceDirectory("/testdata/1.2.18/randomtable/input")                 | 'aeg-00000'    | new File(getResourceDirectory("/testdata/1.2.18/randomtable/aeg_json_output"), 'aeg-00000')                 | true
        getResourceDirectory("/testdata/1.2.18/rangetombstone/input")              | 'aeg-00000'    | new File(getResourceDirectory("/testdata/1.2.18/rangetombstone/aeg_json_output"), 'aeg-00000')              | false
        getResourceDirectory("/testdata/2.0.10/randomtable/input")                 | 'aeg-00000'    | new File(getResourceDirectory("/testdata/2.0.10/randomtable/aeg_json_output"), 'aeg-00000')                 | true
        getResourceDirectory("/testdata/2.0.10_compressed/randomtable/input")      | 'aeg-00000'    | new File(getResourceDirectory("/testdata/2.0.10_compressed/randomtable/aeg_json_output"), 'aeg-00000')      | false
        getResourceDirectory("/testdata/2.0.10/rangetombstone/input")              | 'aeg-00000'    | new File(getResourceDirectory("/testdata/2.0.10/rangetombstone/aeg_json_output"), 'aeg-00000')              | false
        getResourceDirectory("/testdata/2.0.10_1.2.18_combined/randomtable/input") | 'aeg-00000'    | new File(getResourceDirectory("/testdata/2.0.10_1.2.18_combined/randomtable/aeg_json_output"), 'aeg-00000') | true
    }

    @Unroll('Read sstables from cassandra and compact them with aegisthus and generate aegisthus json output from the compacted tables')
    def 'test sstable to compacted sstable to json'() {
        when: 'aegisthus is run'
        def compactedSstableOutput = runAegisthusAndReturnOutputFile([
                inputDirectory: inputDirectory,
                outputSSTableVersion: outputSSTableVersion,
                outputFileName: "keyspace-dataset-${outputSSTableVersion}-0000000000-Data.db",
                produceSSTable: true
        ])

        and: 'then ageisthus is run again on the sstable output'
        def actualOutput = runAegisthusAndReturnOutputFile([
                inputFile     : compactedSstableOutput,
                outputFileName: outputJsonFileName
        ])

        then: 'and the output should match the expected json output'
        compareAegJsonOutput(expectedOutput, actualOutput)
        // Sanity check that the files are identical
        expectedOutput.text == actualOutput.text

        where:
        inputDirectory                                                        | outputSSTableVersion | outputJsonFileName | expectedOutput
        getResourceDirectory("/testdata/1.2.18/randomtable/input")            | 'ic'                 | 'aeg-00000'        | new File(getResourceDirectory("/testdata/1.2.18/randomtable/aeg_json_output"), 'aeg-00000')
        getResourceDirectory("/testdata/1.2.18/rangetombstone/input")         | 'ic'                 | 'aeg-00000'        | new File(getResourceDirectory("/testdata/1.2.18/rangetombstone/aeg_json_output"), 'aeg-00000')
        getResourceDirectory("/testdata/2.0.10/randomtable/input")            | 'jb'                 | 'aeg-00000'        | new File(getResourceDirectory("/testdata/2.0.10/randomtable/aeg_json_output"), 'aeg-00000')
        getResourceDirectory("/testdata/2.0.10_compressed/randomtable/input") | 'jb'                 | 'aeg-00000'        | new File(getResourceDirectory("/testdata/2.0.10_compressed/randomtable/aeg_json_output"), 'aeg-00000')
        getResourceDirectory("/testdata/2.0.10/rangetombstone/input")         | 'jb'                 | 'aeg-00000'        | new File(getResourceDirectory("/testdata/2.0.10/rangetombstone/aeg_json_output"), 'aeg-00000')
    }
}
