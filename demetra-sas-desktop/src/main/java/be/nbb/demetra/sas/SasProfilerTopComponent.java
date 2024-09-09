/*
 * Copyright 2013 National Bank of Belgium
 * 
 * Licensed under the EUPL, Version 1.1 or - as soon they will be approved 
 * by the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 * 
 * http://ec.europa.eu/idabc/eupl
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and 
 * limitations under the Licence.
 */
package be.nbb.demetra.sas;

import ec.nbdemetra.ui.DemetraUI;
import ec.nbdemetra.ui.properties.NodePropertySetBuilder;
import ec.util.various.swing.BasicFileViewer;
import internal.demetra.sas.SasBasicFileHandler;
import java.awt.BorderLayout;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyVetoException;
import java.io.File;
import java.util.Date;
import org.netbeans.api.settings.ConvertAsProperties;
import org.openide.awt.ActionID;
import org.openide.awt.ActionReference;
import org.openide.explorer.ExplorerManager;
import org.openide.explorer.ExplorerUtils;
import org.openide.nodes.AbstractNode;
import org.openide.nodes.Children;
import org.openide.nodes.Node;
import org.openide.nodes.Sheet;
import org.openide.util.Exceptions;
import org.openide.util.ImageUtilities;
import org.openide.util.NbBundle.Messages;
import org.openide.util.lookup.Lookups;
import org.openide.windows.TopComponent;

/**
 * Top component which displays something.
 */
@ConvertAsProperties(
        dtd = "-//be.nbb.demetra.sas.profiler//SasProfiler//EN",
        autostore = false)
@TopComponent.Description(
        preferredID = "SasProfilerTopComponent",
        //iconBase="SET/PATH/TO/ICON/HERE", 
        persistenceType = TopComponent.PERSISTENCE_NEVER)
@TopComponent.Registration(mode = "editor", openAtStartup = false)
@ActionID(category = "Window", id = "be.nbb.demetra.sas.profiler.SasProfilerTopComponent")
@ActionReference(path = "Menu/Tools", position = 543, separatorBefore = 500)
@TopComponent.OpenActionRegistration(
        displayName = "#CTL_SasProfilerAction",
        preferredID = "SasProfilerTopComponent")
@Messages({
    "CTL_SasProfilerAction=Sas Profiler",
    "CTL_SasProfilerTopComponent=Sas Profiler Window",
    "HINT_SasProfilerTopComponent=This is a Sas Profiler window"
})
public final class SasProfilerTopComponent extends TopComponent implements ExplorerManager.Provider {

    private final ExplorerManager mgr = new ExplorerManager();
    private final BasicFileViewer fileViewer;
    private final PropertyChangeListener listener;

    public SasProfilerTopComponent() {
        initComponents();
        setName(Bundle.CTL_SasProfilerTopComponent());
        setIcon(ImageUtilities.loadImage("be/nbb/demetra/sas/sas7bdat.png", true));
        setToolTipText(Bundle.HINT_SasProfilerTopComponent());
        associateLookup(ExplorerUtils.createLookup(mgr, getActionMap()));

        this.listener = new Listener();

        this.fileViewer = new BasicFileViewer();
        fileViewer.setFileHandler(new SasBasicFileHandler());
        fileViewer.addPropertyChangeListener(listener);

        setLayout(new BorderLayout());
        add(fileViewer, BorderLayout.CENTER);
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(this);
        this.setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 400, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 300, Short.MAX_VALUE)
        );
    }// </editor-fold>//GEN-END:initComponents

    // Variables declaration - do not modify//GEN-BEGIN:variables
    // End of variables declaration//GEN-END:variables
    @Override
    public void componentOpened() {
        DemetraUI.getDefault().addPropertyChangeListener(listener);
    }

    @Override
    public void componentClosed() {
        DemetraUI.getDefault().removePropertyChangeListener(listener);
        fileViewer.setFile(null);
    }

    public void setFile(File file) {
        fileViewer.setFile(file);
    }

    void writeProperties(java.util.Properties p) {
        // better to version settings since initial version as advocated at
        // http://wiki.apidesign.org/wiki/PropertyFiles
        p.setProperty("version", "1.0");
    }

    void readProperties(java.util.Properties p) {
        String version = p.getProperty("version");
    }

    @Override
    public ExplorerManager getExplorerManager() {
        return mgr;
    }

    class Listener implements PropertyChangeListener {

        @Override
        public void propertyChange(PropertyChangeEvent evt) {
            switch (evt.getPropertyName()) {
                case BasicFileViewer.FILE_PROPERTY:
                    File model = ((BasicFileViewer) evt.getSource()).getFile();
                    if (model != null) {
                        Node node = new BasicFileViewerNode(model);
                        mgr.setRootContext(node);
                        try {
                            mgr.setSelectedNodes(new Node[]{node});
                        } catch (PropertyVetoException ex) {
                            Exceptions.printStackTrace(ex);
                        }
                    } else {
                        mgr.setRootContext(Node.EMPTY);
                    }
                    break;
                case DemetraUI.COLOR_SCHEME_NAME_PROPERTY:
                    ((SasBasicFileHandler) fileViewer.getFileHandler()).setColorScheme(DemetraUI.getDefault().getColorScheme());
                    break;
            }
        }
    }

    static class BasicFileViewerNode extends AbstractNode {

        public BasicFileViewerNode(File model) {
            super(Children.LEAF, Lookups.singleton(model));
            setName(model.getName());
        }

        @Override
        protected Sheet createSheet() {
            File file = getLookup().lookup(File.class);

            Sheet result = new Sheet();
            NodePropertySetBuilder b = new NodePropertySetBuilder();

            b.with(String.class).selectConst("Name", file.getName()).add();
            b.with(long.class).selectConst("File Size", file.length()).add();
            b.with(Date.class).selectConst("Modification Time", new Date(file.lastModified())).add();
//            b.with(String.class).selectConst("Duration", model.duration + "ms").add();
            result.put(b.build());

            return result;
        }
    }
}
