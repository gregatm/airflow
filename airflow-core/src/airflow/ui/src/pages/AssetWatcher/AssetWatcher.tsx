import { useState } from "react";
import { VStack, Text, Box } from "@chakra-ui/react";
import { useAssetWatcherServiceGetAssetwatcher, useAssetWatcherServiceGetAssetwatcherLog } from "openapi/queries";
import { useParams, useSearchParams } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { useLocalStorage } from "usehooks-ts";
import { SHORTCUTS } from "src/context/keyboardShortcuts";
import { useShortcut } from "src/hooks/useShortcut";
import {
  LOG_SHOW_LOG_LEVEL_KEY,
  LOG_SHOW_SOURCE_KEY,
  LOG_SHOW_TIMESTAMP_KEY,
  LOG_WRAP_KEY,
} from "src/constants/localStorage";
import { TaskLogContent } from "../TaskInstance/Logs/TaskLogContent";
import { TaskLogHeader } from "../TaskInstance/Logs/TaskLogHeader";
import { useConfig } from "src/queries/useConfig";
import { SearchParamsKeys } from "src/constants/searchParams";
import { parseStreamingLogContent } from "src/utils/logs";
import { parseLogs } from "src/queries/useLogs";
import { getDownloadText } from "src/pages/TaskInstance/Logs/utils";
import Time from "src/components/Time";

export const AssetWatcher = () => {
  const { t: translate } = useTranslation();

  const defaultWrap = Boolean(useConfig("default_wrap"));
  const [searchParams, setSearchParams] = useSearchParams();
  const [wrap, setWrap] = useLocalStorage<boolean>(LOG_WRAP_KEY, defaultWrap);
  const [showTimestamp, setShowTimestamp] = useLocalStorage<boolean>(LOG_SHOW_TIMESTAMP_KEY, true);
  const [showSource, setShowSource] = useLocalStorage<boolean>(LOG_SHOW_SOURCE_KEY, false);
  const [showLogLevel, setShowLogLevel] = useLocalStorage<boolean>(LOG_SHOW_LOG_LEVEL_KEY, true);
  const [fullscreen, setFullscreen] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const { watcherId } = useParams();

  const logLevelFilters = searchParams.getAll(SearchParamsKeys.LOG_LEVEL);
  const sourceFilters = searchParams.getAll(SearchParamsKeys.SOURCE);


    const toggleWrap = () => setWrap(!wrap);
    const toggleTimestamp = () => setShowTimestamp(!showTimestamp);
    const toggleLogLevel = () => setShowLogLevel(!showLogLevel);
    const toggleSource = () => setShowSource(!showSource);
    const toggleFullscreen = () => setFullscreen(!fullscreen);
    const toggleExpanded = () => setExpanded((act) => !act);

  const { data: asset, isLoading } = useAssetWatcherServiceGetAssetwatcher({assetwatcherId: watcherId});

  let parsedLogs: any = []
  const logs = useAssetWatcherServiceGetAssetwatcherLog({assetwatcherId: watcherId, dagId: "", dagRunId: "", taskId: ""})
  let parsedData = parseLogs({
    data: parseStreamingLogContent(logs.data),
    logLevelFilters,
    sourceFilters,
    showTimestamp,
    showSource,
    showLogLevel,
    tryNumber: 0,
    translate
    }
  )
  parsedLogs = parsedData.parsedLogs;

  const downloadTextLines = getDownloadText({
    fetchedData: logs.data,
    logLevelFilters,
    showLogLevel,
    showSource,
    showTimestamp,
    sourceFilters,
    translate,
  });

  const getLogString = () => downloadTextLines.filter((line) => line !== "").join("\n");

  const [searchQuery, setSearchQuery] = useState("");
  const [activeSearchIndex, setActiveSearchIndex] = useState(0);

  const handleSearchChange = (query: string) => {
    setSearchQuery(query);
    setActiveSearchIndex(0);
  };


  const downloadLogs = () => {
    const logContent = getLogString();
    const element = document.createElement("a");

    element.href = URL.createObjectURL(new Blob([logContent], { type: "text/plain" }));
    element.download = `logs_${watcherId}.txt`;
    document.body.append(element);
    element.click();
    element.remove();
  };



    useShortcut({
      ...SHORTCUTS.logs.toggleWrap,
      callback: toggleWrap,
      options: { enabled: true },
    });
    useShortcut({
      ...SHORTCUTS.logs.toggleFullscreen,
      callback: toggleFullscreen,
      options: { enabled: true },
    });
    useShortcut({
      ...SHORTCUTS.logs.toggleExpand,
      callback: toggleExpanded,
      options: { enabled: true },
    });
    useShortcut({
      ...SHORTCUTS.logs.toggleTimestamp,
      callback: toggleTimestamp,
      options: { enabled: true },
    });
    useShortcut({
      ...SHORTCUTS.logs.toggleLogLevel,
      callback: toggleLogLevel,
      options: { enabled: true },
    });
    useShortcut({
      ...SHORTCUTS.logs.toggleSource,
      callback: toggleSource,
      options: { enabled: true },
    });
    useShortcut({
      ...SHORTCUTS.logs.downloadLogs,
      callback: downloadLogs,
      options: { enabled: true },
    });

  const logHeaderProps: TaskLogHeaderProps = {
      downloadLogs,
      expanded,
      getLogString,
      search: {
        currentMatchIndex: activeSearchIndex,
        onSearchChange: handleSearchChange,
        searchQuery
      },
      showLogLevel,
      showSource,
      showTimestamp,
      sourceOptions: parsedData.sources,
      toggleExpanded,
      toggleFullscreen,
      toggleLogLevel,
      toggleSource,
      toggleTimestamp,
      toggleWrap,
      wrap,
    };

  return (<>
    { isLoading ? "waiting" : (
      <div>
      <h2>AssetWatcher: {asset.name}</h2>
      <Text>hostname: {asset.trigger.triggerer_job.hostname}</Text>
      <Text>latest_heartbeat: <Time datetime={asset.trigger.triggerer_job.latest_heartbeat}/></Text>
      </div>
      )}
    <TaskLogHeader {...logHeaderProps}></TaskLogHeader>
    <TaskLogContent parsedLogs={parsedLogs}></TaskLogContent>
  </>);
};
