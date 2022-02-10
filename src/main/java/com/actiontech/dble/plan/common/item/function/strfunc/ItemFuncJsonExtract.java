package com.actiontech.dble.plan.common.item.function.strfunc;

import com.actiontech.dble.plan.common.item.Item;
import com.actiontech.dble.plan.common.item.function.ItemFunc;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.*;

/**
 * @author dcy
 * Create Date: 2022-01-24
 */
public class ItemFuncJsonExtract extends ItemStrFunc {
    public static final String ER_INVALID_JSON_PATH = "illegal pattern";
    public static final char SCOPE = '$';
    public static final char BEGIN_MEMBER = '.';
    public static final char BEGIN_ARRAY = '[';
    public static final char END_ARRAY = ']';
    public static final char DOUBLE_QUOTE = '\"';
    public static final char WILDCARD = '*';

    public ItemFuncJsonExtract(List<Item> args, int charsetIndex) {
        super(args, charsetIndex);
    }

    public ItemFuncJsonExtract(Item a, Item b, int charsetIndex) {
        super(a, b, charsetIndex);
    }

    @Override
    public final String funcName() {
        return "json_extract";
    }


    @Override
    public ItemFunc nativeConstruct(List<Item> realArgs) {
        return new ItemFuncJsonExtract(realArgs, charsetIndex);
    }


    @Override
    public String valStr() {
        final Item arg1 = args.get(0);
        if (arg1.isNull()) {
            this.nullValue = true;
            return null;
        }
        String inputStr = arg1.valStr();
        List<String> patterns = new ArrayList<>();
        for (int i = 1; i < args.size(); i++) {
            patterns.add(args.get(i).valStr());
        }
        final String result = jsonExtract(inputStr, patterns);
        if (result == null) {
            this.nullValue = true;
        }
        this.nullValue = false;
        return result;
    }

    private static String jsonExtract(String inputStr, List<String> args) {
        if (inputStr == null) {
            return null;
        }
        Queue<JsonElement> results = new LinkedList<>();
        boolean couldReturnMultipleMatches = args.size() > 1;
        for (int i = 0; i < args.size(); i++) {
            final String arg = args.get(i);
            List<PathLeg> pathLegs = new JsonPath(arg).parsePathLegs();
            final JsonSeeker seeker = new JsonSeeker();

            seeker.seek(inputStr, pathLegs);
            results.addAll(seeker.getResults());
            couldReturnMultipleMatches |= seeker.isCouldReturnMultipleMatches();
        }
        String outputResult;
        if (results.isEmpty()) {
            outputResult = null;
        } else if (!couldReturnMultipleMatches) {
            outputResult = (results.peek().toString());
        } else {
            outputResult = (results.toString());
        }
        return outputResult;
    }


    private static class JsonSeeker {
        boolean couldReturnMultipleMatches = false;
        Stack<JsonElement> results;
        boolean findRecursive;

        private void seek(String inputStr, List<PathLeg> pathLegs) {
            couldReturnMultipleMatches = false;
            results = new Stack<>();
            {
                JsonElement result = new JsonParser().parse(inputStr);
                results.push(result);
            }
            Stack<JsonElement> nextResults;

            final Iterator<PathLeg> pathLegIt = pathLegs.iterator();
            findRecursive = false;
            while (pathLegIt.hasNext()) {

                PathLeg leg = pathLegIt.next();
                if (results.isEmpty()) {
                    break;
                }
                nextResults = new Stack<>();
                switch (leg.getLegType()) {
                    case JPL_MEMBER: {
                        while (!results.isEmpty()) {
                            JsonElement result = results.pop();
                            if (result.isJsonObject()) {
                                final JsonObject jsonObject = result.getAsJsonObject();

                                if (jsonObject.has(leg.getMemberProperty())) {
                                    nextResults.add(jsonObject.get(leg.getMemberProperty()));
                                }

                            }
                            if (findRecursive) {
                                addAllProperties(results, result);
                            }
                        }
                        findRecursive = false;
                    }
                    break;
                    case JPL_MEMBER_WILDCARD: {

                        couldReturnMultipleMatches = true;

                        while (!results.isEmpty()) {
                            JsonElement result = results.pop();
                            if (result.isJsonObject()) {
                                final JsonObject jsonObject = result.getAsJsonObject();
                                for (Map.Entry<String, JsonElement> entry : jsonObject.entrySet()) {
                                    nextResults.add(entry.getValue());
                                }
                            }
                            if (findRecursive) {
                                addAllProperties(results, result);
                            }
                        }


                        findRecursive = false;
                    }
                    break;

                    case JPL_ARRAY_CELL: {
                        while (!results.isEmpty()) {
                            JsonElement result = results.pop();
                            if (result.isJsonArray()) {
                                final JsonArray jsonArray = result.getAsJsonArray();

                                if (leg.getArrayCell() < jsonArray.size()) {
                                    nextResults.add(jsonArray.get(leg.getArrayCell()));
                                }

                            }

                            if (findRecursive) {
                                addAllProperties(results, result);
                            }

                        }
                        findRecursive = false;
                    }
                    break;

                    case JPL_ARRAY_CELL_WILDCARD: {
                        couldReturnMultipleMatches = true;
                        while (!results.isEmpty()) {
                            JsonElement result = results.pop();

                            if (result.isJsonArray()) {
                                final JsonArray jsonArray = result.getAsJsonArray();
                                jsonArray.forEach(nextResults::add);

                            }

                            if (findRecursive) {
                                addAllProperties(results, result);
                            }

                        }
                        findRecursive = false;
                    }
                    break;

                    case JPL_ELLIPSIS:
                        nextResults = processEllipsis(pathLegIt);
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
                results = nextResults;
            }


        }

        private Stack<JsonElement> processEllipsis(Iterator<PathLeg> pathLegIt) {
            couldReturnMultipleMatches = true;
            if (findRecursive && !pathLegIt.hasNext()) {
                throw new IllegalStateException("Invalid JSON path expression.");
            }
            findRecursive = true;
            Stack<JsonElement> nextResults = results;
            return nextResults;
        }

        private static void addAllProperties(Collection<JsonElement> results, JsonElement result) {
            if (result.isJsonObject()) {
                for (Map.Entry<String, JsonElement> entry : result.getAsJsonObject().entrySet()) {
                    results.add(entry.getValue());
                }
            } else if (result.isJsonArray()) {
                result.getAsJsonArray().forEach(results::add);
            } else {
                //find nothing
            }
        }

        public boolean isCouldReturnMultipleMatches() {
            return couldReturnMultipleMatches;
        }

        public Stack<JsonElement> getResults() {
            return results;
        }

    }

    private static class JsonPath {
        int index = 0;
        char[] pattern;

        JsonPath(String pattern) {
            this.pattern = pattern.toCharArray();
        }

        public List<PathLeg> parsePathLegs() {
            List<PathLeg> legs = new ArrayList<>();
            try {
                purgeWhitespace();
                if (pattern[index++] != SCOPE) {
                    throw new IllegalStateException("illegal pattern");
                }

                while (index < pattern.length) {
                    purgeWhitespace();
                    PathLeg pathLeg = parsePathLeg();
                    legs.add(pathLeg);
                }
            } catch (ArrayIndexOutOfBoundsException ex) {
                throw new IllegalStateException("illegal pattern", ex);
            }
            return legs;
        }

        private void purgeWhitespace() {
            while (Character.isWhitespace(pattern[index])) {
                index++;
            }
        }

        private PathLeg parsePathLeg() {


            PathLeg pathLeg;

            char b = pattern[index];
            switch (b) {
                case BEGIN_ARRAY: {
                    pathLeg = parseArrayLeg();
                }
                break;
                case BEGIN_MEMBER: {
                    pathLeg = parseMemberLeg();
                }
                break;
                case WILDCARD: {
                    pathLeg = parseEllipsisLeg();
                }
                break;
                default:
                    throw new IllegalStateException("illegal pattern");
            }
            return pathLeg;
        }

        private PathLeg parseEllipsisLeg() {
            index++;

            if (pattern[index++] != WILDCARD) {
                throw new IllegalStateException("illegal pattern");
            }
            purgeWhitespace();
            //** without suffix is illegal
            if (index == pattern.length) {
                throw new IllegalStateException("illegal pattern");
            }
            //'***' is illegal
            if (pattern[index] == WILDCARD) {
                throw new IllegalStateException("illegal pattern");
            }

            PathLeg leg = PathLeg.ofType(JsonPathLegType.JPL_ELLIPSIS);
            return leg;
        }

        private PathLeg parseMemberLeg() {
            index++;
            purgeWhitespace();
            PathLeg leg;
            int beginIndex = index;
            if (pattern[index] == WILDCARD) {
                index++;
                leg = PathLeg.ofType(JsonPathLegType.JPL_MEMBER_WILDCARD);
            } else {
                findEndOfMemberName();
                int endIndex = index;
                boolean wasQuoted = (pattern[beginIndex] == DOUBLE_QUOTE);
                String tmpS;
                if (wasQuoted) {
                    tmpS = new String(pattern, beginIndex, endIndex - beginIndex);

                } else {
                    StringBuilder sb = new StringBuilder();
                    tmpS = sb.append(DOUBLE_QUOTE).append(pattern, beginIndex, endIndex - beginIndex).append(DOUBLE_QUOTE).toString();
                }
                tmpS = new JsonParser().parse(tmpS).getAsString();
                leg = PathLeg.ofMemberProperty(tmpS);
            }
            return leg;
        }

        private void findEndOfMemberName() {
            if (pattern[index] == DOUBLE_QUOTE) {
                index++;
                while (index < pattern.length) {
                    switch (pattern[index++]) {
                        case '\\':
                            /*
                              Skip the next character after a backslash. It cannot mark
                              the end of the quoted string.
                            */
                            index++;
                            break;
                        case DOUBLE_QUOTE:
                            // An unescaped double quote marks the end of the quoted string.
                            return;
                        default:
                            continue;
                    }
                }
                return;
            }

            while (index < pattern.length &&
                    !Character.isWhitespace(pattern[index]) &&
                    pattern[index] != BEGIN_ARRAY &&
                    pattern[index] != BEGIN_MEMBER &&
                    pattern[index] != WILDCARD) {
                index++;
            }

        }

        private PathLeg parseArrayLeg() {
            index++;
            purgeWhitespace();
            PathLeg leg;
            if (pattern[index] == WILDCARD) {
                index++;
                leg = PathLeg.ofType(JsonPathLegType.JPL_ARRAY_CELL_WILDCARD);

            } else {
                int beginIndex = index;
                for (; index < pattern.length; index++) {
                    if (pattern[index] < '0' || pattern[index] > '9') {
                        break;
                    }
                }
                if (beginIndex == index || index == pattern.length) {
                    throw new IllegalStateException(ER_INVALID_JSON_PATH);
                }

                int endIndex = index;
                final int number = Integer.parseInt(new String(pattern, beginIndex, endIndex - beginIndex));
                leg = PathLeg.ofArrayCell(number);
            }
            purgeWhitespace();
            if (pattern[index++] != END_ARRAY) {
                throw new IllegalStateException("illegal pattern");
            }
            return leg;
        }

    }

    static final class PathLeg {
        JsonPathLegType legType;
        int arrayCell;
        String memberProperty;


        private PathLeg(String memberProperty) {
            this.legType = JsonPathLegType.JPL_MEMBER;
            this.memberProperty = memberProperty;
        }

        private PathLeg(int arrayCell) {
            this.legType = JsonPathLegType.JPL_ARRAY_CELL;
            this.arrayCell = arrayCell;
        }

        private PathLeg(JsonPathLegType legType) {
            this.legType = legType;
        }

        public static PathLeg ofType(JsonPathLegType legType) {
            return new PathLeg(legType);
        }

        public static PathLeg ofMemberProperty(String memberProperty) {
            return new PathLeg(memberProperty);
        }

        public static PathLeg ofArrayCell(int arrayCell) {
            return new PathLeg(arrayCell);
        }


        public JsonPathLegType getLegType() {
            return legType;
        }

        public int getArrayCell() {
            return arrayCell;
        }

        public String getMemberProperty() {
            return memberProperty;
        }
    }

    enum JsonPathLegType {
        JPL_MEMBER,
        JPL_ARRAY_CELL,
        JPL_MEMBER_WILDCARD,
        JPL_ARRAY_CELL_WILDCARD,
        JPL_ELLIPSIS
    }
}
