package be.nbb.demetra.sas.file.actions;

import be.nbb.demetra.sas.SasProfilerTopComponent;
import be.nbb.demetra.sas.file.SasFileBean;
import be.nbb.demetra.sas.file.SasFileProvider;
import com.google.common.base.Optional;
import ec.nbdemetra.ui.NbComponents;
import ec.nbdemetra.ui.tsproviders.DataSourceNode;
import ec.tss.tsproviders.DataSource;
import ec.tss.tsproviders.TsProviders;
import ec.tstoolkit.utilities.Files2;
import ec.ui.ExtAction;
import internal.jd3.AbilityNodeAction3;
import org.openide.awt.ActionID;
import org.openide.awt.ActionReference;
import org.openide.awt.ActionRegistration;
import org.openide.util.NbBundle.Messages;
import org.openide.util.actions.Presenter;

import javax.swing.*;
import java.io.File;
import java.util.Objects;
import java.util.stream.Stream;

@ActionID(category = "Edit", id = ProfileFileAction.ID)
@ActionRegistration(displayName = "#CTL_ProfileFileAction", lazy = false)
@Messages("CTL_ProfileFileAction=Profile file")
@ActionReference(path = DataSourceNode.ACTION_PATH, position = 1720, separatorBefore = 1700, id = @ActionID(category = "Edit", id = ProfileFileAction.ID))
public final class ProfileFileAction extends AbilityNodeAction3<DataSource> implements Presenter.Popup {

    static final String ID = "be.nbb.demetra.sas.file.actions.ProfileFileAction";

    public ProfileFileAction() {
        super(DataSource.class, true);
    }

    @Override
    public JMenuItem getPopupPresenter() {
        return ExtAction.hideWhenDisabled(new JMenuItem(this));
    }

    @Override
    protected void performAction(Stream<DataSource> items) {
        items
                .map(ProfileFileAction::getSas7bdatFileOrNull)
                .filter(Objects::nonNull)
                .forEach(ProfileFileAction::open);
    }

    private static void open(File file) {
        String name = ID + file.getPath();
        SasProfilerTopComponent c = NbComponents.findTopComponentByNameAndClass(name, SasProfilerTopComponent.class);
        if (c == null) {
            c = new SasProfilerTopComponent();
            c.setName(name);
            c.setFile(file);
            c.setDisplayName(file.getName());
            c.open();
        }
        c.requestActive();
    }

    @Override
    protected boolean enable(Stream<DataSource> items) {
        return items.anyMatch(dataSource -> getSas7bdatFileOrNull(dataSource) != null);
    }

    private static File getSas7bdatFileOrNull(DataSource dataSource) {
        Optional<SasFileProvider> loader = TsProviders.lookup(SasFileProvider.class, dataSource.getProviderName());
        if (loader.isPresent()) {
            SasFileBean bean = loader.get().decodeBean(dataSource);
            File file = bean.getFile();
            File realFile = Files2.getAbsoluteFile(loader.get().getPaths(), file);
            return realFile != null && realFile.isDirectory() ? new File(realFile, bean.getTable()) : realFile;
        }
        return null;
    }

    @Override
    public String getName() {
        return Bundle.CTL_ProfileFileAction();
    }
}
